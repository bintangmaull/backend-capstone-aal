# app/service/service_gempa.py

import logging
import pandas as pd
from scipy.interpolate import CubicSpline
from app.repository.repo_kurva_gempa import get_reference_curves_gempa
from app.extensions import db
from app.models.models_database import HasilProsesGempa

logger = logging.getLogger(__name__)

def to_float(v):
    """Helper: None if NaN/None, else Python float."""
    if pd.isna(v):
        return None
    return float(v)

def interpolate_spline(x, y, xi):
    """CubicSpline + clamp [0,1]."""
    if pd.isna(xi):
        return None
    try:
        spline = CubicSpline(x, y, extrapolate=True)
        val = spline(float(xi))
        return float(max(0, min(val, 1)))
    except Exception as e:
        logger.error(f"❌ ERROR interpolasi nilai {xi}: {e}")
        return None

def process_data(input_data):
    """
    Proses data Gempa: interpolasi CR, MCF, MUR, Lightwood untuk MMI500/250/100.
    Simpan ke dmgratio_gempa (bulk insert/update).
    """
    logger.info("📥 Mulai interpolasi data Gempa...")
    rc = get_reference_curves_gempa()
    if not rc:
        logger.warning("⚠️ Kurva Gempa kosong, dibatalkan.")
        return pd.DataFrame()

    df = input_data.copy()
    for c in ['MMI500','MMI250','MMI100']:
        df[c] = pd.to_numeric(df[c], errors='coerce')

    # interpolasi
    for tipe, ref in rc.items():
        x_ref, y_ref = ref['x'], ref['y']
        logger.info(f"📊 Kurva {tipe}: X={x_ref}, Y={y_ref}")
        for m in ['500','250','100']:
            in_col, out_col = f'MMI{m}', f'dmgratio_{tipe.lower()}_mmi{m}'
            df[out_col] = df[in_col].apply(lambda v: interpolate_spline(x_ref, y_ref, v))

    # enforce cr ≤ mcf ≤ mur ≤ lightwood
    for m in ['500','250','100']:
        cr, mcf, mur, lw = (
            f'dmgratio_cr_mmi{m}',
            f'dmgratio_mcf_mmi{m}',
            f'dmgratio_mur_mmi{m}',
            f'dmgratio_lightwood_mmi{m}',
        )
        df[mcf] = df[[cr, mcf]].max(axis=1)
        df[mur] = df[[mcf, mur]].max(axis=1)
        df[lw]  = df[[mur, lw]].max(axis=1)

    # siapkan result & cast Python float/None
    cols = ['id_lokasi'] + [
        f'dmgratio_{t.lower()}_mmi{m}'
        for t in rc.keys() for m in ['500','250','100']
    ]
    result = df[cols].applymap(to_float)
    logger.info(f"✅ Interpolasi selesai: {len(result)} baris.")

    # bulk insert/update
    try:
        existing = {i for (i,) in db.session.query(HasilProsesGempa.id_lokasi).all()}
        to_ins, to_upd = [], []

        for _, row in result.iterrows():
            rec = HasilProsesGempa(
                id_lokasi = int(row['id_lokasi']),
                dmgratio_cr_mmi500        = to_float(row['dmgratio_cr_mmi500']),
                dmgratio_mcf_mmi500       = to_float(row['dmgratio_mcf_mmi500']),
                dmgratio_mur_mmi500       = to_float(row['dmgratio_mur_mmi500']),
                dmgratio_lightwood_mmi500 = to_float(row['dmgratio_lightwood_mmi500']),
                dmgratio_cr_mmi250        = to_float(row['dmgratio_cr_mmi250']),
                dmgratio_mcf_mmi250       = to_float(row['dmgratio_mcf_mmi250']),
                dmgratio_mur_mmi250       = to_float(row['dmgratio_mur_mmi250']),
                dmgratio_lightwood_mmi250 = to_float(row['dmgratio_lightwood_mmi250']),
                dmgratio_cr_mmi100        = to_float(row['dmgratio_cr_mmi100']),
                dmgratio_mcf_mmi100       = to_float(row['dmgratio_mcf_mmi100']),
                dmgratio_mur_mmi100       = to_float(row['dmgratio_mur_mmi100']),
                dmgratio_lightwood_mmi100 = to_float(row['dmgratio_lightwood_mmi100']),
            )
            (to_upd if rec.id_lokasi in existing else to_ins).append(rec)

        with db.session.no_autoflush:
            if to_ins:
                db.session.bulk_save_objects(to_ins)
                logger.info(f"✅ {len(to_ins)} records inserted.")
            if to_upd:
                for rec in to_upd:
                    ex = db.session.get(HasilProsesGempa, rec.id_lokasi)
                    # assign ulang
                    ex.dmgratio_cr_mmi500        = rec.dmgratio_cr_mmi500
                    ex.dmgratio_mcf_mmi500       = rec.dmgratio_mcf_mmi500
                    ex.dmgratio_mur_mmi500       = rec.dmgratio_mur_mmi500
                    ex.dmgratio_lightwood_mmi500 = rec.dmgratio_lightwood_mmi500
                    ex.dmgratio_cr_mmi250        = rec.dmgratio_cr_mmi250
                    ex.dmgratio_mcf_mmi250       = rec.dmgratio_mcf_mmi250
                    ex.dmgratio_mur_mmi250       = rec.dmgratio_mur_mmi250
                    ex.dmgratio_lightwood_mmi250 = rec.dmgratio_lightwood_mmi250
                    ex.dmgratio_cr_mmi100        = rec.dmgratio_cr_mmi100
                    ex.dmgratio_mcf_mmi100       = rec.dmgratio_mcf_mmi100
                    ex.dmgratio_mur_mmi100       = rec.dmgratio_mur_mmi100
                    ex.dmgratio_lightwood_mmi100 = rec.dmgratio_lightwood_mmi100
                logger.info(f"✅ {len(to_upd)} records updated.")

        db.session.commit()
        logger.info("✅ Semua perubahan tersimpan di tabel dmgratio_gempa.")
    except Exception as e:
        db.session.rollback()
        logger.error(f"❌ Gagal simpan data Gempa: {e}")

    return result
