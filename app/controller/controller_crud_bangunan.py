import logging
from flask import request, jsonify
from app.service.service_crud_bangunan import BangunanService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BangunanController:
    @staticmethod
    def get_all():
        try:
            prov = request.args.get('provinsi')
            kota = request.args.get('kota')
            nama = request.args.get('nama')
            data = BangunanService.get_all_bangunan(prov, kota, nama)
            return jsonify(data), 200
        except Exception as e:
            logger.error(f"Error get_all: {e}")
            return jsonify({"error": "Terjadi kesalahan server"}), 500

    @staticmethod
    def get_by_id(bangunan_id):
        try:
            data = BangunanService.get_bangunan_by_id(bangunan_id)
            if data:
                return jsonify(data), 200
            return jsonify({"error": "Bangunan tidak ditemukan"}), 404
        except Exception as e:
            logger.error(f"Error get_by_id: {e}")
            return jsonify({"error": "Terjadi kesalahan server"}), 500

    @staticmethod
    def create():
        try:
            data = request.get_json() or {}
            required = [
                "id_bangunan", "lon", "lat", "taxonomy",
                "luas", "nama_gedung", "alamat", "kota", "provinsi"
            ]
            missing = [f for f in required if f not in data]
            if missing:
                return jsonify({
                    "error": f"Kolom wajib tidak lengkap: {', '.join(missing)}"
                }), 400

            new_b = BangunanService.create_bangunan(data)
            return jsonify(new_b), 201
        except Exception as e:
            logger.error(f"Error saat menambahkan bangunan: {e}")
            return jsonify({"error": "Terjadi kesalahan server"}), 500

    @staticmethod
    def update(bangunan_id):
        try:
            data = request.get_json() or {}
            data.pop("id_bangunan", None)
            data.pop("geom", None)
            upd = BangunanService.update_bangunan(bangunan_id, data)
            if upd:
                return jsonify(upd), 200
            return jsonify({"error": "Bangunan tidak ditemukan"}), 404
        except Exception as e:
            logger.error(f"Error saat mengedit bangunan: {e}")
            return jsonify({"error": "Terjadi kesalahan server"}), 500

    @staticmethod
    def delete(bangunan_id, prov):
        try:
            ok = BangunanService.delete_bangunan(bangunan_id, prov)
            if ok:
                return jsonify({"message": "Bangunan berhasil dihapus"}), 200
            return jsonify({"error": "Bangunan tidak ditemukan"}), 404
        except Exception as e:
            logger.error(f"Error saat menghapus bangunan: {e}")
            return jsonify({"error": "Terjadi kesalahan server"}), 500

    @staticmethod
    def new_id():
        taxonomy = request.args.get('taxonomy')
        if taxonomy not in ("BMN", "FS", "FD"):
            return jsonify({"error": "taxonomy invalid"}), 400
        try:
            new_id = BangunanService.generate_unique_id(taxonomy)
            return jsonify({"id_bangunan": new_id}), 200
        except Exception as e:
            logger.error(f"Error new_id: {e}")
            return jsonify({"error": "Terjadi kesalahan server"}), 500

    @staticmethod
    def get_provinsi_list():
        try:
            data = BangunanService.get_provinsi_list()
            return jsonify(data), 200
        except Exception as e:
            logger.error(f"Error get_provinsi_list: {e}")
            return jsonify([]), 500

    @staticmethod
    def get_kota_list():
        prov = request.args.get('provinsi')
        if not prov:
            return jsonify([]), 400
        try:
            data = BangunanService.get_kota_list(prov)
            return jsonify(data), 200
        except Exception as e:
            logger.error(f"Error get_kota_list: {e}")
            return jsonify([]), 500

    @staticmethod
    def upload_csv():
        """
        POST /api/bangunan/upload
        Form-field 'file': CSV tanpa kolom geom, dengan header sesuai _fields di repo.
        """
        if 'file' not in request.files:
            return jsonify({"error": "File CSV harus di-upload"}), 400
        file = request.files['file']
        if file.filename == "":
            return jsonify({"error": "Nama file kosong"}), 400

        logger.info("Mulai upload CSV: %s", file.filename)
        try:
            result = BangunanService.upload_csv(file)
            logger.info("Upload CSV selesai sukses: %s", result)
            return jsonify(result), 200

        except ValueError as ve:
            # Kesalahan validasi atau parsing CSV
            logger.error("Error upload CSV (ValueError): %s", ve)
            return jsonify({"error": str(ve)}), 400

        except Exception as e:
            # Kesalahan tak terduga
            logger.exception("Exception saat processing CSV upload")
            return jsonify({"error": "Terjadi kesalahan server saat upload CSV"}), 500

    @staticmethod
    def recalc(bangunan_id):
        """
        POST /api/bangunan/<id>/recalc
        Hitung ulang directloss & AAL hanya untuk bangunan ini.
        """
        try:
            # service_crud_bangunan harus memiliki metode recalc_building
            result = BangunanService.recalc_building_directloss_and_aal(bangunan_id)
            return jsonify({"status": "success", "detail": result}), 200
        except ValueError as ve:
            logger.error(f"Error recalc (ValueError): {ve}")
            return jsonify({"error": str(ve)}), 400
        except Exception as e:
            logger.error(f"Error recalc bangunan: {e}")
            return jsonify({"error": "Terjadi kesalahan perhitungan ulang"}), 500
