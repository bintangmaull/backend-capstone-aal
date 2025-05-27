from app.repository.repo_crud_hsbgn import HSBGNRepository

class HSBGNService:
    @staticmethod
    def get_all_hsbgn():
        """Mengambil semua data HSBGN"""
        return [h.to_dict() for h in HSBGNRepository.get_all()]

    @staticmethod
    def get_hsbgn_by_id(hsbgn_id):
        """Mengambil HSBGN berdasarkan ID"""
        hsbgn = HSBGNRepository.get_by_id(hsbgn_id)
        return hsbgn.to_dict() if hsbgn else None

    @staticmethod
    def get_hsbgn_by_kota(kota):
        """Mengambil HSBGN berdasarkan Kota"""
        return [h.to_dict() for h in HSBGNRepository.get_by_kota(kota)]

    @staticmethod
    def create_hsbgn(data):
        """Menambahkan HSBGN baru"""
        return HSBGNRepository.create(data).to_dict()

    @staticmethod
    def update_hsbgn(hsbgn_id, data):
        """Memperbarui HSBGN"""
        hsbgn = HSBGNRepository.update(hsbgn_id, data)
        return hsbgn.to_dict() if hsbgn else None

    @staticmethod
    def delete_hsbgn(hsbgn_id):
        """Menghapus HSBGN"""
        return HSBGNRepository.delete(hsbgn_id)
