from app.models.models_database import GunungBerapiReferenceCurve
from app.extensions import db
import logging

logger = logging.getLogger(__name__)

def get_reference_curves_gunungberapi():
    """Mengambil referensi kurva Gunung Berapi dari database dengan transaksi yang aman."""
    try:
        logger.info("📥 Mengambil referensi kurva Gunung Berapi dari database...")

        # **Gunakan sesi dengan pengelolaan yang lebih aman**
        with db.session.no_autoflush:  # Menghindari nested transaction error
            curves = db.session.query(GunungBerapiReferenceCurve).all()

        # **Pastikan session ditutup setelah query**
        db.session.expunge_all()
        db.session.close()

        reference_curves = {}
        for curve in curves:
            if curve.tipe_kurva not in reference_curves:
                reference_curves[curve.tipe_kurva] = {"x": [], "y": []}
            reference_curves[curve.tipe_kurva]["x"].append(curve.x)
            reference_curves[curve.tipe_kurva]["y"].append(curve.y)

        logger.info(f"✅ Berhasil mengambil {len(curves)} referensi kurva Gunung Berapi.")
        return reference_curves

    except Exception as e:
        db.session.rollback()  # Pastikan transaksi dibatalkan jika error
        db.session.close()  # Tutup sesi untuk menghindari kebocoran
        logger.error(f"❌ ERROR: Gagal mengambil referensi kurva Gunung Berapi dari database: {e}")
        return {}  # Kembalikan dictionary kosong jika gagal
