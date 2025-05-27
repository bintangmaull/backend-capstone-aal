import json, logging
from app.repository.repo_buffer_hazard import get_buffered_features, get_nearest_point

logger = logging.getLogger(__name__)

class BufferDisasterService:

    @staticmethod
    def get_feature_collection(dtype: str, field: str, bbox: dict, tol: float):
        """
        :param dtype: jenis bencana
        :param field: kolom intensitas yang diminta
        :param bbox: bounding box dict
        :param tol: simplify tolerance
        """
        # panggil repo dengan signature baru: dtype, field, bbox, tol
        rows = get_buffered_features(dtype, field, bbox, tol)
        features = []
        for row in rows:
            geojson_str = row.geojson
            if not geojson_str:
                continue
            geom = json.loads(geojson_str)
            mapping = getattr(row, "_mapping", row)
            # hanya satu properti sesuai lazy‑load field
            props = { field: mapping["value"] }

            features.append({
                "type": "Feature",
                "properties": props,
                "geometry": geom
            })
        return {"type":"FeatureCollection","features":features}

    @staticmethod
    def get_nearest(dtype: str, field: str, lat: float, lng: float):
        """
        :param dtype: jenis bencana
        :param field: kolom intensitas yang diminta
        :param lat,lng: koordinat untuk nearest lookup
        """
        return get_nearest_point(dtype, field, lat, lng)
