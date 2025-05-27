# app/service/service_visualisasi_kurva.py

from app.models.models_database import (
    GempaReferenceCurve,
    BanjirReferenceCurve,
    GunungBerapiReferenceCurve,
    LongsorReferenceCurve
)

def get_all_disaster_curves():
    # Peta nama tabel ke Model SQLAlchemy
    mapping = {
        "gempa": GempaReferenceCurve,
        "banjir": BanjirReferenceCurve,
        "gunungberapi": GunungBerapiReferenceCurve,
        "longsor": LongsorReferenceCurve
    }

    all_curves = {}
    for disaster, Model in mapping.items():
        # ambil semua baris, sort dulu by tipe_kurva lalu by x
        rows = Model.query.order_by(Model.tipe_kurva, Model.x).all()

        # kelompokkan per tipe_kurva
        grouped = {}
        for row in rows:
            t = row.tipe_kurva
            if t not in grouped:
                grouped[t] = {"x": [], "y": []}
            grouped[t]["x"].append(row.x)
            grouped[t]["y"].append(row.y)

        all_curves[disaster] = grouped

    return all_curves
