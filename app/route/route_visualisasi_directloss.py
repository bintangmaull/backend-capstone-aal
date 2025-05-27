import io
import math
from flask import Blueprint, request, Response, jsonify
from app.service.service_visualisasi_directloss import GedungService

gedung_bp = Blueprint('gedung', __name__, url_prefix='/api')

# — GeoJSON endpoints (lama) —
@gedung_bp.route('/gedung', methods=['GET'])
def get_gedung():
    bbox = request.args.get('bbox')
    prov = request.args.get('provinsi')
    kota = request.args.get('kota')
    geojson = GedungService.get_geojson(bbox, prov, kota)
    return jsonify(geojson)

@gedung_bp.route('/provinsi', methods=['GET'])
def list_provinsi():
    return jsonify(GedungService.get_provinsi_list())

@gedung_bp.route('/kota', methods=['GET'])
def list_kota():
    prov = request.args.get('provinsi')
    if not prov:
        return jsonify([]), 400
    return jsonify(GedungService.get_kota_list(prov))

@gedung_bp.route('/aal-provinsi', methods=['GET'])
def get_aal_geojson():
    prov = request.args.get('provinsi')
    geojson = GedungService.get_aal_geojson(prov)
    return jsonify(geojson)

@gedung_bp.route('/aal-provinsi-list', methods=['GET'])
def list_aal_provinsi():
    return jsonify(GedungService.get_aal_provinsi_list())

@gedung_bp.route('/aal-provinsi-data', methods=['GET'])
def aal_data():
    prov = request.args.get('provinsi')
    if not prov:
        return jsonify({"error": "provinsi required"}), 400
    data = GedungService.get_aal_data(prov)
    if not data:
        return jsonify({}), 404
    # sanitize NaN / Inf
    for k, v in data.items():
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            data[k] = 0.0
    return jsonify(data)

# — CSV download endpoints (tanpa filter) —
@gedung_bp.route('/gedung/download', methods=['GET'])
def download_directloss():
    """
    Stream CSV seluruh tabel hasil_proses_directloss + bangunan tanpa filter.
    Termasuk kolom nama_gedung dan alamat dari tabel bangunan.
    """
    # Ambil cursor psycopg2
    from app.extensions import db
    raw_conn = db.session.connection().connection
    cur = raw_conn.cursor()

    # Sertakan nama_gedung dan alamat
    copy_sql = """
    COPY (
      SELECT
        b.id_bangunan,
        b.nama_gedung,
        b.alamat,
        b.kota,
        b.provinsi,
        b.luas,
        b.taxonomy,
        b.jumlah_lantai,
        d.*
      FROM bangunan_copy b
      JOIN hasil_proses_directloss d USING (id_bangunan)
    ) TO STDOUT WITH CSV HEADER
    """

    def generate():
        buf = io.StringIO()
        cur.copy_expert(copy_sql, buf)
        buf.seek(0)
        while True:
            chunk = buf.read(8192)
            if not chunk:
                break
            yield chunk

    return Response(
        generate(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=directloss.csv'}
    )

@gedung_bp.route('/aal-provinsi/download', methods=['GET'])
def download_aal():
    """
    Stream CSV seluruh tabel hasil_aal_provinsi tanpa filter.
    """
    from app.extensions import db
    raw_conn = db.session.connection().connection
    cur = raw_conn.cursor()

    copy_sql = """
    COPY (
      SELECT *
      FROM hasil_aal_provinsi
      ORDER BY provinsi
    ) TO STDOUT WITH CSV HEADER
    """

    def generate():
        buf = io.StringIO()
        cur.copy_expert(copy_sql, buf)
        buf.seek(0)
        while True:
            chunk = buf.read(8192)
            if not chunk:
                break
            yield chunk

    return Response(
        generate(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=aal_provinsi.csv'}
    )


def setup_visualisasi_routes(app):
    app.register_blueprint(gedung_bp)
