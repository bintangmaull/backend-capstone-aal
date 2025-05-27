from flask import Blueprint, request, jsonify
from app.service.service_visualisasi_directloss import GedungService

gedung_bp = Blueprint('gedung', __name__, url_prefix='/api')

@gedung_bp.route('/gedung', methods=['GET'])
def get_gedung():
    """
    GET /api/gedung
      query params:
        - bbox=minx,miny,maxx,maxy   (opsional)
        - provinsi=[nama provinsi]    (opsional)
        - kota=[nama kota]            (opsional)
    """
    bbox = request.args.get('bbox')
    prov = request.args.get('provinsi')
    kota = request.args.get('kota')

    geojson = GedungService.get_geojson(bbox, prov, kota)
    return jsonify(geojson)
