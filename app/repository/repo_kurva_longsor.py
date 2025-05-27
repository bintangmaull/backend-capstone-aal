# app/repository/repo_kurva_longsor.py

import logging
from app.models.models_database import LongsorReferenceCurve
from app.extensions import db

logger = logging.getLogger(__name__)

def get_reference_curves_longsor():
    """Mengambil & memproses referensi kurva Longsor dari DB."""
    try:
        logger.info("📥 Mengambil referensi kurva Longsor dari database...")
        with db.session.no_autoflush:
            curves = (
                db.session
                  .query(LongsorReferenceCurve)
                  .order_by(LongsorReferenceCurve.tipe_kurva,
                            LongsorReferenceCurve.x)
                  .all()
            )
        db.session.expunge_all()
        db.session.close()

        reference_curves = {}
        for curve in curves:
            key = curve.tipe_kurva.strip().upper()
            reference_curves.setdefault(key, {"x": [], "y": []})
            reference_curves[key]["x"].append(curve.x)
            reference_curves[key]["y"].append(curve.y)

        # Sort & hapus duplikat x (strictly increasing)
        for key, data in reference_curves.items():
            unique = []
            last_x = None
            for x_val, y_val in zip(data["x"], data["y"]):
                if x_val != last_x:
                    unique.append((x_val, y_val))
                    last_x = x_val
            xs, ys = zip(*unique)
            reference_curves[key]["x"] = list(xs)
            reference_curves[key]["y"] = list(ys)

        logger.info(f"✅ Berhasil mengambil {len(curves)} referensi kurva Longsor.")
        return reference_curves

    except Exception as e:
        db.session.rollback()
        db.session.close()
        logger.error(f"❌ ERROR mengambil kurva Longsor: {e}")
        return {}
