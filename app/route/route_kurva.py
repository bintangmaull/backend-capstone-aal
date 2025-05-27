from ..controller.controller_kurva import (
    process_kurva_gempa,
    process_kurva_banjir,
    process_kurva_longsor,
    process_kurva_gunungberapi
)

def setup_kurva_routes(app):
    """
    Menetapkan route API untuk pemrosesan kurva dari berbagai bencana.
    """
    app.add_url_rule('/process_kurva_gempa', 'process_kurva_gempa', process_kurva_gempa, methods=['POST'])
    app.add_url_rule('/process_kurva_banjir', 'process_kurva_banjir', process_kurva_banjir, methods=['POST'])
    app.add_url_rule('/process_kurva_longsor', 'process_kurva_longsor', process_kurva_longsor, methods=['POST'])
    app.add_url_rule('/process_kurva_gunungberapi', 'process_kurva_gunungberapi', process_kurva_gunungberapi, methods=['POST'])
