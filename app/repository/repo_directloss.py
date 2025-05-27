# app/repository/repo_directloss.py

import os
import pandas as pd
from sqlalchemy import create_engine, text
from app.config import Config

# Direktori Debug (opsional)
DEBUG_DIR = os.path.join(os.getcwd(), "debug_output")
os.makedirs(DEBUG_DIR, exist_ok=True)

def get_db_connection():
    """Membuat koneksi ke database PostgreSQL."""
    try:
        return create_engine(Config.SQLALCHEMY_DATABASE_URI)
    except Exception as e:
        raise ConnectionError(f"❌ Gagal terhubung ke database: {e}")

def get_bangunan_data():
    """Mengambil data bangunan lengkap."""
    query = text("""
        SELECT
            b.id_bangunan,
            b.geom,
            b.luas,
            b.nama_gedung,
            b.alamat,
            b.kode_bangunan,
            b.provinsi,
            b.kota,
            b.jumlah_lantai,
            COALESCE(k.hsbgn, 0.0) AS hsbgn
        FROM bangunan_copy b
        LEFT JOIN kota k ON b.kota = k.kota;
    """)
    engine = get_db_connection()
    with engine.connect() as conn:
        return pd.read_sql(query, conn)

def get_all_disaster_data():
    """
    Untuk tiap jenis bencana:
     - Filter titik intensitas dalam 'threshold' (meter via geography)
     - Cari nearest dengan KNN (<->) di geography
     - Ambil nilai vulnerability via dmgratio_*
    """
    def vcols_gempa(pre, s):
        return [
            f"h.dmgratio_cr_{pre}{s}         AS nilai_y_cr_{pre}{s}",
            f"h.dmgratio_mcf_{pre}{s}        AS nilai_y_mcf_{pre}{s}",
            f"h.dmgratio_mur_{pre}{s}        AS nilai_y_mur_{pre}{s}",
            f"h.dmgratio_lightwood_{pre}{s}  AS nilai_y_lightwood_{pre}{s}",
        ]

    def vcols_banjir(pre, s):
        return [
            f"h.dmgratio_1_{pre}{s} AS nilai_y_1_{pre}{s}",
            f"h.dmgratio_2_{pre}{s} AS nilai_y_2_{pre}{s}",
        ]

    mapping = {
        "gempa": {
            "raw":      "model_intensitas_gempa",
            "dmgr":     "dmgratio_gempa",
            "prefix":   "mmi",
            "scales":   ["500","250","100"],
            "threshold": 9500,
            "vcols":    vcols_gempa
        },
        "banjir": {
            "raw":      "model_intensitas_banjir",
            "dmgr":     "dmgratio_banjir_copy",
            "prefix":   "depth",
            "scales":   ["100","50","25"],
            "threshold": 700,
            "vcols":    vcols_banjir
        },
        "longsor": {
            "raw":      "model_intensitas_longsor",
            "dmgr":     "dmgratio_longsor",
            "prefix":   "mflux",
            "scales":   ["5","2"],
            "threshold": 700,
            "vcols":    vcols_gempa
        },
        "gunungberapi": {
            "raw":      "model_intensitas_gunungberapi",
            "dmgr":     "dmgratio_gunungberapi",
            "prefix":   "kpa",
            "scales":   ["250","100","50"],
            "threshold": 550,
            "vcols":    vcols_gempa
        }
    }

    engine = get_db_connection()
    all_data = {}

    with engine.connect() as conn:
        for name, cfg in mapping.items():
            raw_table  = cfg["raw"]
            dmgr_table = cfg["dmgr"]
            pre        = cfg["prefix"]
            scales     = cfg["scales"]
            threshold  = cfg["threshold"]
            make_vcols = cfg["vcols"]

            subq_parts = []
            sel_parts  = []
            for s in scales:
                for expr in make_vcols(pre, s):
                    subq_parts.append(expr)
                    alias = expr.split(" AS ")[1]
                    sel_parts.append(f"near.{alias}")

            subq_cols   = ",\n       ".join(subq_parts)
            outer_cols  = ",\n  ".join(sel_parts)

            sql = f"""
                SELECT
                  b.id_bangunan,
                  {outer_cols}
                FROM bangunan_copy b
                JOIN LATERAL (
                  SELECT
                    {subq_cols}
                  FROM {raw_table} r
                  JOIN {dmgr_table} h USING(id_lokasi)
                  WHERE ST_DWithin(
                    b.geom::geography,
                    r.geom::geography,
                    {threshold}
                  )
                  ORDER BY b.geom::geography <-> r.geom::geography
                  LIMIT 1
                ) AS near ON TRUE;
            """

            df = pd.read_sql(text(sql), conn)
            all_data[name] = df

    return all_data
