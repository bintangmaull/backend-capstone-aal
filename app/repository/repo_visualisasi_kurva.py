from app.models.models_database import (
    GempaReferenceCurve,
    BanjirReferenceCurve,
    GunungBerapiReferenceCurve,
    LongsorReferenceCurve,
)

def get_disaster_data(disaster_type):
    model_map = {
        "gempa": GempaReferenceCurve,
        "banjir": BanjirReferenceCurve,
        "gunungberapi": GunungBerapiReferenceCurve,
        "longsor": LongsorReferenceCurve,
    }

    model = model_map.get(disaster_type)
    if not model:
        raise ValueError(f"Unknown disaster type: {disaster_type}")

    rows = model.query.order_by(model.x.asc()).all()
    return [(row.x, row.y) for row in rows]
