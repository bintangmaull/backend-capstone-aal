from flask import jsonify
import pandas as pd

from app.service.service_kurva_gempa import process_data as process_gempa
from app.service.service_kurva_banjir import process_data as process_banjir
from app.service.service_kurva_longsor import process_data as process_longsor
from app.service.service_kurva_gunungberapi import process_data as process_gunungberapi

from app.models.models_database import (
    RawGempa, HasilProsesGempa,
    RawBanjir, HasilProsesBanjir,
    RawLongsor, HasilProsesLongsor,
    RawGunungBerapi, HasilProsesGunungBerapi
)
from app.extensions import db

# ======================== GEMPA ========================
def process_kurva_gempa():
    try:
        raw = RawGempa.query.all()
        if not raw:
            return jsonify({"error": "No data found in raw_gempa table"}), 404

        df = pd.DataFrame([r.to_dict() for r in raw])
        df.rename(columns={
            'mmi_500': 'MMI500',
            'mmi_250': 'MMI250',
            'mmi_100': 'MMI100'
        }, inplace=True)

        output = process_gempa(df)
        output.to_csv("output_kurva_gempa.csv", index=False)

        save_to_database(output, HasilProsesGempa)
        return jsonify({
            "status": "success",
            "message": "Gempa data successfully processed and saved to database",
            "processed_data_count": len(output)
        })

    except Exception as e:
        return jsonify({"error": f"Processing error: {str(e)}"}), 500


# ======================== BANJIR ========================
def process_kurva_banjir():
    try:
        raw = RawBanjir.query.all()
        if not raw:
            return jsonify({"error": "No data found in raw_gempa table"}), 404

        df = pd.DataFrame([r.to_dict() for r in raw])
        df.rename(columns={
            'depth_100': 'depth_100',
            'depth_50': 'depth_50',
            'depth_25': 'depth_25'
        }, inplace=True)

        output = process_banjir(df)
        output.to_csv("output_kurva_banjir.csv", index=False)

        save_to_database(output, HasilProsesBanjir)
        return jsonify({
            "status": "success",
            "message": "Gempa data successfully processed and saved to database",
            "processed_data_count": len(output)
        })

    except Exception as e:
        return jsonify({"error": f"Processing error: {str(e)}"}), 500



# ======================== LONGSOR ========================
def process_kurva_longsor():
    try:
        raw = RawLongsor.query.all()
        if not raw:
            return jsonify({"error": "No data found in raw_longsor table"}), 404

        df = pd.DataFrame([r.to_dict() for r in raw])
        df = df[['id_lokasi', 'mflux_5', 'mflux_2']].copy()
        for col in ['mflux_5', 'mflux_2']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        output = process_longsor(df)
        output.to_csv("output_kurva_longsor.csv", index=False)

        save_to_database(output, HasilProsesLongsor)
        return jsonify({
            "status": "success",
            "message": "Longsor data successfully processed and saved to database",
            "processed_data_count": len(output)
        })

    except Exception as e:
        return jsonify({"error": f"Processing error: {str(e)}"}), 500


# ======================== GUNUNG BERAPI ========================
def process_kurva_gunungberapi():
    try:
        raw = RawGunungBerapi.query.all()
        if not raw:
            return jsonify({"error": "No data found in raw_gunungberapi table"}), 404

        df = pd.DataFrame([r.to_dict() for r in raw])
        df = df[['id_lokasi', 'kpa_250', 'kpa_100', 'kpa_50']].copy()
        for col in ['kpa_250', 'kpa_100', 'kpa_50']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        output = process_gunungberapi(df)
        output.to_csv("output_kurva_gunungberapi.csv", index=False)

        save_to_database(output, HasilProsesGunungBerapi)
        return jsonify({
            "status": "success",
            "message": "Gunung Berapi data successfully processed and saved to database",
            "processed_data_count": len(output)
        })

    except Exception as e:
        return jsonify({"error": f"Processing error: {str(e)}"}), 500

# ======================== FUNGSI SIMPAN DATABASE ========================
def save_to_database(output_data, model_class, clear_old_data=True):
    try:
        if clear_old_data:
            db.session.query(model_class).delete()
            db.session.commit()

        output_data = output_data.astype(float)

        new_records = [
            model_class(**row.to_dict())
            for _, row in output_data.iterrows()
        ]

        db.session.bulk_save_objects(new_records)
        db.session.commit()

        print(f"✅ {len(new_records)} records saved to {model_class.__tablename__}")

    except Exception as e:
        db.session.rollback()
        print(f"❌ Error saving to database: {e}")
        raise



