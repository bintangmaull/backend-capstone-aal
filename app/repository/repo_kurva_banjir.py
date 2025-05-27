# app/repository/repo_kurva_banjir.py

import logging
from app.extensions import db
from app.models.models_database import BanjirReferenceCurve

logger = logging.getLogger(__name__)

def get_reference_curves_banjir():
    """
    Ambil semua referensi kurva Banjir,
    normalize tipe_kurva ke '1' atau '2',
    lalu kembalikan hanya data untuk kedua tipe itu.
    """
    try:
        logger.info("📥 Mengambil referensi kurva Banjir...")
        curves = db.session.query(BanjirReferenceCurve).all()

        # Inisialisasi struktur untuk tipe '1' & '2'
        reference_curves = {'1': {'x': [], 'y': []},
                            '2': {'x': [], 'y': []}}

        for c in curves:
            raw = c.tipe_kurva
            # Normalize:
            if isinstance(raw, (int, float)):
                t = str(int(raw))
            else:
                ts = str(raw).strip()
                # Jika '1.0' atau '2.0', ambil sebelum titik
                if '.' in ts and ts.replace('.', '', 1).isdigit():
                    t = ts.split('.', 1)[0]
                else:
                    t = ts

            if t in reference_curves:
                reference_curves[t]['x'].append(c.x)
                reference_curves[t]['y'].append(c.y)

        total = len(curves)
        logger.info(f"✅ Ditemukan {total} baris referensi (tipe 1 & 2 dipakai).")
        return reference_curves

    except Exception as e:
        logger.error(f"❌ Gagal ambil referensi kurva Banjir: {e}")
        db.session.rollback()
        return {'1': {'x': [], 'y': []}, '2': {'x': [], 'y': []}}

    finally:
        db.session.close()
