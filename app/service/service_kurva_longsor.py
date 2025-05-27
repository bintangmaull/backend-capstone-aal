# app/service/service_kurva_longsor.py

import logging
import pandas as pd
from scipy.interpolate import CubicSpline
from app.repository.repo_kurva_longsor import get_reference_curves_longsor
from app.extensions import db
from app.models.models_database import HasilProsesLongsor

logger = logging.getLogger(__name__)

def to_float(v):
    """Helper: None jika NaN/None, else Python float."""
    if pd.isna(v):
        return None
    return float(v)

def interpolate_cubic_with_linear_extrap(x_ref, y_ref, xi):
    """
    - <2 titik: y_ref[0] jika ada
    - xi di dalam domain: CubicSpline(extrapolate=False)
    - xi di luar domain: linear extrap berdasarkan 2 titik terdekat
    - Clamp ke [0,1]
    """
    if pd.isna(xi):
        return None

    xs, ys = x_ref, y_ref
    n = len(xs)
    if n < 2:
        val = ys[0] if ys else None
    else:
        x0, x1 = xs[0], xs[1]
        xn_1, xn = xs[-2], xs[-1]
        if xi < x0:
            slope = (ys[1] - ys[0]) / (x1 - x0)
            val = ys[0] + slope * (xi - x0)
        elif xi > xn:
            slope = (ys[-1] - ys[-2]) / (xn - xn_1)
            val = ys[-1] + slope * (xi - xn)
        else:
            try:
                spline = CubicSpline(xs, ys, extrapolate=False)
                val = float(spline(float(xi)))
            except Exception as e:
                logger.error(f"❌ ERROR spline interior {xi}: {e}")
                # fallback ke linear interp agar tidak None
                val = float(pd.Series(ys).interpolate().reindex(pd.Series(xs).append(pd.Series([xi]))).iloc[-1])

    return float(max(0, min(val, 1))) if val is not None else None

def process_data(input_data):
    """
    Proses data Longsor (mflux_5, mflux_2):
    - CubicSpline interior + linear extrapolasi luar domain
    - clamp [0,1]
    - enforce CR≤MCF≤MUR≤LIGHTWOOD
    - bulk insert/update ke dmgratio_longsor
    """
    logger.info("📥 Mulai interpolasi data Longsor...")
    rc = get_reference_curves_longsor()
    if not rc:
        logger.warning("⚠️ Kurva Longsor kosong, dibatalkan.")
        return pd.DataFrame()

    df = input_data.copy()
    for c in ['mflux_5','mflux_2']:
        df[c] = pd.to_numeric(df[c], errors='coerce')

    # interpolasi per tipe & skala
    for tipe, ref in rc.items():
        xs, ys = ref['x'], ref['y']
        logger.info(f"📊 Kurva {tipe}: X={xs}, Y={ys}")
        for m in ['5','2']:
            in_col = f'mflux_{m}'
            out_col= f'dmgratio_{tipe.lower()}_mflux{m}'
            df[out_col] = df[in_col].apply(
                lambda v: interpolate_cubic_with_linear_extrap(xs, ys, v)
            )

    # enforce ordering: cr ≤ mcf ≤ mur ≤ lightwood
    for m in ['5','2']:
        cr, mcf, mur, lw = (
            f'dmgratio_cr_mflux{m}',
            f'dmgratio_mcf_mflux{m}',
            f'dmgratio_mur_mflux{m}',
            f'dmgratio_lightwood_mflux{m}',
        )
        df[mcf] = df[[cr, mcf]].max(axis=1)
        df[mur] = df[[mcf, mur]].max(axis=1)
        df[lw]  = df[[mur, lw]].max(axis=1)

    # siapkan DataFrame hasil dan cast Python float/None
    cols = ['id_lokasi'] + [
        f'dmgratio_{t.lower()}_mflux{m}'
        for t in rc.keys() for m in ['5','2']
    ]
    result = df[cols].applymap(to_float)
    logger.info(f"✅ Interpolasi selesai: {len(result)} baris.")

    # bulk insert/update
    try:
        existing = {i for (i,) in db.session.query(HasilProsesLongsor.id_lokasi).all()}
        to_ins, to_upd = [], []

        # pisahkan insert vs update
        for _, row in result.iterrows():
            vals = {col: row[col] for col in cols if col != 'id_lokasi'}
            if row['id_lokasi'] in existing:
                to_upd.append((int(row['id_lokasi']), vals))
            else:
                to_ins.append((int(row['id_lokasi']), vals))

        with db.session.no_autoflush:
            # bulk insert
            if to_ins:
                objs = [
                    HasilProsesLongsor(id_lokasi=idl, **vals)
                    for idl, vals in to_ins
                ]
                db.session.bulk_save_objects(objs)
                logger.info(f"✅ {len(objs)} records inserted.")

            # update satu-per-satu
            if to_upd:
                for idl, vals in to_upd:
                    ex = db.session.get(HasilProsesLongsor, idl)
                    for col, v in vals.items():
                        setattr(ex, col, v)  # v sudah Python float/None
                logger.info(f"✅ {len(to_upd)} records updated.")

        db.session.commit()
        logger.info("✅ Semua perubahan berhasil disimpan di tabel dmgratio_longsor.")
    except Exception as e:
        db.session.rollback()
        logger.error(f"❌ Gagal simpan data Longsor: {e}")

    return result
