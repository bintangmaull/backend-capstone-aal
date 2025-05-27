# app/controller/controller_visualisasi_kurva.py

from flask import jsonify
from app.service.service_visualisasi_kurva import get_all_disaster_curves

def get_disaster_curves_controller():
    data = get_all_disaster_curves()   # sekarang nested sesuai harapan frontend
    return jsonify(data)
