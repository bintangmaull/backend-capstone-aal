from app.controller.controller_directloss import home, process_data

def setup_join_routes(app):
    """
    Menetapkan rute API untuk pemrosesan data.
    """
    app.add_url_rule('/', 'home', home, methods=['GET'])
    app.add_url_rule('/process_join', 'process_data', process_data, methods=['GET'])
