from app.models.models_database import HSBGN
from app.extensions import db

class HSBGNRepository:
    @staticmethod
    def get_all():
        """Mengambil semua data HSBGN"""
        return HSBGN.query.all()

    @staticmethod
    def get_by_id(hsbgn_id):
        """Mengambil HSBGN berdasarkan ID"""
        # id_kota di database bertipe string, jadi cast hsbgn_id ke string
        return HSBGN.query.filter(HSBGN.id_kota == str(hsbgn_id)).first()

    @staticmethod
    def get_by_kota(kota):
        """Mengambil HSBGN berdasarkan kota"""
        return HSBGN.query.filter(HSBGN.kota.ilike(f"%{kota}%")).all()

    @staticmethod
    def create(data):
        """Menambahkan data baru ke tabel HSBGN"""
        new_hsbgn = HSBGN(**data)
        db.session.add(new_hsbgn)
        db.session.commit()
        return new_hsbgn

    @staticmethod
    def update(hsbgn_id, data):
        """Memperbarui data HSBGN berdasarkan ID"""
        # gunakan filter string untuk mencocokkan id_kota
        hsbgn = HSBGN.query.filter(HSBGN.id_kota == str(hsbgn_id)).first()
        if hsbgn:
            for key, value in data.items():
                setattr(hsbgn, key, value)
            db.session.commit()
            return hsbgn
        return None

    @staticmethod
    def delete(hsbgn_id):
        """Menghapus data HSBGN berdasarkan ID"""
        # gunakan filter string untuk mencocokkan id_kota
        hsbgn = HSBGN.query.filter(HSBGN.id_kota == str(hsbgn_id)).first()
        if hsbgn:
            db.session.delete(hsbgn)
            db.session.commit()
            return True
        return False