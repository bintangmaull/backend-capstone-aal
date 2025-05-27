# app/service/service_kurva_banjir.py

import logging
import pandas as pd
from scipy.interpolate import CubicSpline

from app.repository.repo_kurva_banjir import get_reference_curves_banjir
from app.extensions import db
from app.models.models_database import HasilProsesBanjir

logger = logging.getLogger(__name__)

def interpolate_spline(x, y, xi):
    """Interpolasi CubicSpline, dibatasi di [0,1]."""
    if pd.isna(xi):
        return None
    try:
        spline = CubicSpline(x, y, extrapolate=True)
        val = spline(float(xi))
        return float(max(0, min(val, 1)))
    except Exception as e:
        logger.error(f"❌ Error interpolasi nilai {xi}: {e}")
        return None

def process_data(input_data: pd.DataFrame) -> pd.DataFrame:
    """
    Untuk setiap baris input_data:
      - ambil referensi kurva tipe '1' & '2'
      - interpolasi depth_100, 50, 25
      - hasilkan kolom dmgratio_1_* dan dmgratio_2_*
    """
    logger.info("📥 Memulai proses interpolasi Banjir...")

    # 1) Ambil referensi (tipe '1' & '2')
    reference_curves = get_reference_curves_banjir()

    # 2) Salin dan cast kolom depth
    df = input_data.copy()
    for col in ['depth_100', 'depth_50', 'depth_25']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 3) Inisialisasi kolom hasil
    for tipe in ['1','2']:
        for d in ['100','50','25']:
            df[f'dmgratio_{tipe}_depth{d}'] = None

    # 4) Interpolasi untuk tiap tipe
    for tipe, ref in reference_curves.items():
        x_ref, y_ref = ref['x'], ref['y']
        if not x_ref or not y_ref:
            logger.warning(f"⚠️ Referensi tipe {tipe} kosong: seluruh dmgratio_{tipe}_* = None")
            continue

        logger.info(f"📊 Interpolasi kurva tipe {tipe} (n={len(x_ref)})")
        for d in ['100','50','25']:
            in_col  = f'depth_{d}'
            out_col = f'dmgratio_{tipe}_depth{d}'
            df[out_col] = df[in_col].apply(lambda v: interpolate_spline(x_ref, y_ref, v))

    # 5) Pilih kolom final
    cols = ['id_lokasi'] + [
        f'dmgratio_{t}_depth{d}'
        for t in ['1','2'] for d in ['100','50','25']
    ]
    result = df[cols]

    # 6) Simpan ke database (bulk: hapus lalu insert)
    try:
        db.session.query(HasilProsesBanjir).delete()
        recs = result.to_dict(orient='records')
        objs = [HasilProsesBanjir(**r) for r in recs]
        db.session.bulk_save_objects(objs)
        db.session.commit()
        logger.info(f"✅ {len(objs)} records saved to {HasilProsesBanjir.__tablename__}")
    except Exception as e:
        db.session.rollback()
        logger.error(f"❌ Gagal simpan dmgratio_banjir_copy: {e}")

    return result
