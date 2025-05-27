from flask import Blueprint
from app.controller.controller_kurva import (
    process_kurva_gempa,
    process_kurva_banjir,
    process_kurva_longsor,
    process_kurva_gunungberapi
)

# Buat Blueprint
main_bp = Blueprint('main', __name__)

# Route Utama
@main_bp.route('/')
def index():
    return {"message": "Welcome to the main route!"}

# Route untuk setiap jenis bencana
@main_bp.route('/process_kurva_gempa', methods=['GET'])
def process_gempa():
    return process_kurva_gempa()

@main_bp.route('/process_kurva_banjir', methods=['GET'])
def process_banjir():
    return process_kurva_banjir()

@main_bp.route('/process_kurva_longsor', methods=['GET'])
def process_longsor():
    return process_kurva_longsor()

@main_bp.route('/process_kurva_gunungberapi', methods=['GET'])
def process_gunungberapi():
    return process_kurva_gunungberapi()
