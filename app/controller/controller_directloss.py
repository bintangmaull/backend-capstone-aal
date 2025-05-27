from flask import jsonify
from app.service.service_directloss import process_all_disasters

def home():
    """Endpoint utama API untuk mengecek apakah server berjalan."""
    return jsonify({"message": "Flask API is running!"}), 200


def process_data():
    """Mengambil data dari database, memprosesnya, dan menyimpannya kembali ke database & CSV"""
    try:
        result_path = process_all_disasters()
        return jsonify({
            "status": "success",
            "message": "Data berhasil diproses dan disimpan ke database & CSV",
            "file_path": result_path
        }), 200
    except Exception as e:
        return jsonify({"error": f"Processing error: {str(e)}"}), 500
