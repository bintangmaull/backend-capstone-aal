from app.models.models_database import RawGempa, RawBanjir, RawLongsor, RawGunungBerapi
from app import db
from geoalchemy2.shape import to_shape

class IntensitasRepo:
    @staticmethod
    def get_points_by_bencana(bencana, kolom):
        model_map = {
            'gempa': RawGempa,
            'banjir': RawBanjir,
            'longsor': RawLongsor,
            'gunungberapi': RawGunungBerapi
        }

        model = model_map.get(bencana)
        if not model:
            return []

        try:
            results = db.session.query(model).all()
            points = []
            for row in results:
                geom = to_shape(row.geom)
                points.append({
                    'id_lokasi': row.id_lokasi,
                    'x': geom.x,
                    'y': geom.y,
                    kolom: getattr(row, kolom, None)
                })
            return points
        except Exception as e:
            print(f"[ERROR] Gagal mengambil data {bencana}: {e}")
            return []
