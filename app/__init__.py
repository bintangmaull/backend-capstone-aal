import os
import logging
from flask import Flask
from sqlalchemy import text
from app.config import Config
from app.extensions import db, migrate
from flask_cors import CORS

# CRUD & raw-data blueprints
from app.route.route_raw import main_bp
from app.route.route_crud_bangunan import bangunan_bp
from app.route.route_crud_hsbgn import hsbgn_bp

# Visualization (direct-loss) blueprint
from app.route.route_visualisasi_directloss import setup_visualisasi_routes
from app.route.route_directloss import setup_join_routes
from app.route.route_visualisasi_hazard import register_visualisasi_routes_hazard

# (Optional) preload curves
from app.repository.repo_kurva_gempa import get_reference_curves_gempa as load_gempa
from app.repository.repo_kurva_gunungberapi import get_reference_curves_gunungberapi as load_gunung
from app.repository.repo_kurva_longsor import get_reference_curves_longsor as load_longsor
from app.repository.repo_kurva_banjir import get_reference_curves_banjir as load_banjir

# visualisasi kurva
from app.route.route_visualisasi_kurva import disaster_curve_bp

# coba gempa hazard
from app.route.route_buffer_hazard import bp as buffer_disaster_bp
#from app.route.route_visualisasi_hazard import bencana_bp  # Hapus import ini agar tidak didaftarkan 2x

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Globals for curves
REFERENCE_CURVES_GEMPA = {}
REFERENCE_CURVES_GUNUNGBERAPI = {}
REFERENCE_CURVES_LONGSOR = {}
REFERENCE_CURVES_BANJIR = {}

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    CORS(app, origins=["http://localhost:3000"])

    # ensure upload folder
    upload_folder = app.config.get('UPLOAD_FOLDER',
                                   os.path.join(app.root_path, 'uploads'))
    os.makedirs(upload_folder, exist_ok=True)
    app.config['UPLOAD_FOLDER'] = upload_folder

    # init extensions
    db.init_app(app)
    migrate.init_app(app, db)

    # register CRUD & raw-data blueprints
    app.register_blueprint(main_bp)
    app.register_blueprint(bangunan_bp)
    app.register_blueprint(hsbgn_bp)
    app.register_blueprint(disaster_curve_bp)
    # Hapus pendaftaran langsung bencana_bp karena sudah didaftarkan via register_visualisasi_routes_hazard
    # app.register_blueprint(bencana_bp)

    # test
    app.register_blueprint(buffer_disaster_bp)
    logger.info("✅ CRUD & raw-data blueprints registered")

    # register visualization routes (/api/gedung, /api/provinsi, /api/kota)
    setup_visualisasi_routes(app)
    # register process_join route
    setup_join_routes(app)
    logger.info("✅ Direct-loss visualization routes registered")

    # Daftarkan blueprint bencana_bp lewat fungsi khusus agar tidak dobel
    register_visualisasi_routes_hazard(app)

    # preload curves & check DB connection
    with app.app_context():
        _load_reference_curves()
        if app.debug:
            _check_db_connection()

    return app

def _check_db_connection():
    try:
        db.session.execute(text("SELECT 1"))
        logger.info("✅ Database connected successfully")
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")

def _load_reference_curves():
    global REFERENCE_CURVES_GEMPA, REFERENCE_CURVES_GUNUNGBERAPI
    global REFERENCE_CURVES_LONGSOR, REFERENCE_CURVES_BANJIR

    try:
        REFERENCE_CURVES_GEMPA = load_gempa()
        logger.info("✅ Gempa curves loaded")
    except Exception as e:
        logger.error(f"❌ Failed to load gempa curves: {e}")
        REFERENCE_CURVES_GEMPA = {}

    try:
        REFERENCE_CURVES_GUNUNGBERAPI = load_gunung()
        logger.info("✅ Gunung Berapi curves loaded")
    except Exception as e:
        logger.error(f"❌ Failed to load gunung berapi curves: {e}")
        REFERENCE_CURVES_GUNUNGBERAPI = {}

    try:
        REFERENCE_CURVES_LONGSOR = load_longsor()
        logger.info("✅ Longsor curves loaded")
    except Exception as e:
        logger.error(f"❌ Failed to load longsor curves: {e}")
        REFERENCE_CURVES_LONGSOR = {}

    try:
        REFERENCE_CURVES_BANJIR = load_banjir()
        logger.info("✅ Banjir curves loaded")
    except Exception as e:
        logger.error(f"❌ Failed to load banjir curves: {e}")
        REFERENCE_CURVES_BANJIR = {}
