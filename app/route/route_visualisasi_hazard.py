from app.controller.controller_visualisasi_hazard import bencana_bp

def register_visualisasi_routes_hazard(app):
    app.register_blueprint(bencana_bp)
