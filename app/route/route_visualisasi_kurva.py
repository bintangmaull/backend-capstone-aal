# app/route/route_visualisasi_kurva.py

from flask import Blueprint
from app.controller.controller_visualisasi_kurva import get_disaster_curves_controller

# Pastikan Blueprint ini bernama disaster_curve_bp
disaster_curve_bp = Blueprint('visualisasi_kurva', __name__)

# Daftarkan route pada objek disaster_curve_bp
disaster_curve_bp.route('/api/disaster-curves', methods=['GET'])(
    get_disaster_curves_controller
)
