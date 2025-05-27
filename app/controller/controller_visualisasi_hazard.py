from flask import Blueprint, jsonify
from app.service.service_visualisasi_hazard import RasterService
from app.geoserver_register import upload_all_geotiffs
bencana_bp = Blueprint('bencana_bp', __name__)

@bencana_bp.route('/generate-raster/<bencana>/<kolom>', methods=['GET'])
def generate_raster(bencana, kolom):
    allowed_bencana = ['gempa', 'banjir', 'longsor', 'gunungberapi']
    if bencana not in allowed_bencana:
        return jsonify({'status': 'error', 'message': 'Jenis bencana tidak valid'}), 400

    raster_path, error = RasterService.generate_raster_from_points(bencana, kolom)
    if error:
        return jsonify({'status': 'error', 'message': error}), 404

    return jsonify({'status': 'success', 'raster_file': raster_path})


@bencana_bp.route('/generate-all-raster', methods=['GET'])
def generate_all_raster():
    bencana_map = {
        'gempa': ['mmi_100', 'mmi_250', 'mmi_500'],
        'banjir': ['depth_100', 'depth_50', 'depth_25'],
        'longsor': ['mflux_5', 'mflux_2'],
        'gunungberapi': ['kpa_50', 'kpa_100', 'kpa_250']
    }

    hasil = []

    for bencana, koloms in bencana_map.items():
        for kolom in koloms:
            try:
                path, error = RasterService.generate_raster_from_points(bencana, kolom)
                if error:
                    hasil.append({
                        'bencana': bencana,
                        'kolom': kolom,
                        'status': 'error',
                        'message': error
                    })
                else:
                    hasil.append({
                        'bencana': bencana,
                        'kolom': kolom,
                        'status': 'success',
                        'raster_file': path
                    })
            except Exception as e:
                hasil.append({
                    'bencana': bencana,
                    'kolom': kolom,
                    'status': 'error',
                    'message': str(e)
                })

    return jsonify(hasil)


@bencana_bp.route('/geoserver/upload-all', methods=['GET'])
def upload_all_to_geoserver():
    """
    Generate semua raster sebagai GeoTIFF dan upload ke GeoServer
    via REST PUT external.geotiff
    """
    results = upload_all_geotiffs()
    return jsonify(results)