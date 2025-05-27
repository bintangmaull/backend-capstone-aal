import logging
import pandas as pd
from scipy.interpolate import CubicSpline
from app.repository.repo_kurva_gunungberapi import get_reference_curves_gunungberapi
from app.extensions import db
from app.models.models_database import HasilProsesGunungBerapi  # ORM model untuk tabel dmgratio_gunungberapi

# Setup logging
logger = logging.getLogger(__name__)

def interpolate_spline(x, y, xi):
    """Interpolasi CubicSpline dengan hasil dibatasi [0, 1]."""
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
    Proses data kpa untuk interpolasi CR, MCF, MUR, Lightwood pada Gunung Berapi.
    Kolom input: lon, lat, kpa_250, kpa_100, kpa_50.
    Output: DataFrame hasil interpolasi.
    """
    logger.info("📥 Memulai proses interpolasi data Gunung Berapi...")

    # Pastikan selalu kembalikan DataFrame, minimal kosong
    result = pd.DataFrame()

    reference_curves = get_reference_curves_gunungberapi()
    if not reference_curves:
        logger.warning("⚠️ Tidak ada referensi kurva Gunung Berapi! Proses dihentikan.")
        return result   # DataFrame kosong, bukan None

    df = input_data.copy()
    for col in ['kpa_250', 'kpa_100', 'kpa_50']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Lakukan interpolasi per tipe kurva
    for tipe, ref in reference_curves.items():
        x_ref, y_ref = ref['x'], ref['y']
        logger.info(f"📊 Referensi {tipe}: X={x_ref}, Y={y_ref}")
        for kpa in ['250', '100', '50']:
            col_in  = f'kpa_{kpa}'
            col_out = f'dmgratio_{tipe.lower()}_kpa{kpa}'
            df[col_out] = df[col_in].apply(lambda v: interpolate_spline(x_ref, y_ref, v))

    # Kolom keluaran sesuai HasilProsesGunungBerapi
    cols = [
        'id_lokasi',
        'dmgratio_cr_kpa250', 'dmgratio_mcf_kpa250', 'dmgratio_mur_kpa250', 'dmgratio_lightwood_kpa250',
        'dmgratio_cr_kpa100', 'dmgratio_mcf_kpa100', 'dmgratio_mur_kpa100', 'dmgratio_lightwood_kpa100',
        'dmgratio_cr_kpa50',  'dmgratio_mcf_kpa50',  'dmgratio_mur_kpa50',  'dmgratio_lightwood_kpa50',
    ]
    result = df[cols].applymap(lambda x: float(x) if pd.notna(x) else None)
    logger.info(f"✅ Interpolasi selesai: {result.shape[0]} baris.")

    # — Bulk insert/update ke DB —
    try:
        existing_ids = {id_ for (id_,) in db.session.query(HasilProsesGunungBerapi.id_lokasi).all()}
        to_insert, to_update = [], []

        for _, row in result.iterrows():
            rec = HasilProsesGunungBerapi(
                id_lokasi=int(row['id_lokasi']),
                dmgratio_cr_kpa250=float(row['dmgratio_cr_kpa250']),
                dmgratio_mcf_kpa250=float(row['dmgratio_mcf_kpa250']),
                dmgratio_mur_kpa250=float(row['dmgratio_mur_kpa250']),
                dmgratio_lightwood_kpa250=float(row['dmgratio_lightwood_kpa250']),
                dmgratio_cr_kpa100=float(row['dmgratio_cr_kpa100']),
                dmgratio_mcf_kpa100=float(row['dmgratio_mcf_kpa100']),
                dmgratio_mur_kpa100=float(row['dmgratio_mur_kpa100']),
                dmgratio_lightwood_kpa100=float(row['dmgratio_lightwood_kpa100']),
                dmgratio_cr_kpa50=float(row['dmgratio_cr_kpa50']),
                dmgratio_mcf_kpa50=float(row['dmgratio_mcf_kpa50']),
                dmgratio_mur_kpa50=float(row['dmgratio_mur_kpa50']),
                dmgratio_lightwood_kpa50=float(row['dmgratio_lightwood_kpa50']),
            )
            if rec.id_lokasi in existing_ids:
                to_update.append(rec)
            else:
                to_insert.append(rec)

        # Commit manual, tanpa nested transaction
        if to_insert:
            db.session.bulk_save_objects(to_insert)
            logger.info(f"✅ {len(to_insert)} records inserted.")
        if to_update:
            for rec in to_update:
                existing = db.session.get(HasilProsesGunungBerapi, rec.id_lokasi)
                existing.dmgratio_cr_kpa250 = rec.dmgratio_cr_kpa250
                existing.dmgratio_mcf_kpa250 = rec.dmgratio_mcf_kpa250
                existing.dmgratio_mur_kpa250 = rec.dmgratio_mur_kpa250
                existing.dmgratio_lightwood_kpa250 = rec.dmgratio_lightwood_kpa250
                existing.dmgratio_cr_kpa100 = rec.dmgratio_cr_kpa100
                existing.dmgratio_mcf_kpa100 = rec.dmgratio_mcf_kpa100
                existing.dmgratio_mur_kpa100 = rec.dmgratio_mur_kpa100
                existing.dmgratio_lightwood_kpa100 = rec.dmgratio_lightwood_kpa100
                existing.dmgratio_cr_kpa50  = rec.dmgratio_cr_kpa50
                existing.dmgratio_mcf_kpa50 = rec.dmgratio_mcf_kpa50
                existing.dmgratio_mur_kpa50 = rec.dmgratio_mur_kpa50
                existing.dmgratio_lightwood_kpa50 = rec.dmgratio_lightwood_kpa50
            logger.info(f"✅ {len(to_update)} records updated.")

        db.session.commit()
        logger.info("✅ Semua perubahan tersimpan di tabel dmgratio_gunungberapi.")
    except Exception as e:
        db.session.rollback()
        logger.error(f"❌ Gagal simpan: {e}")

    return result
