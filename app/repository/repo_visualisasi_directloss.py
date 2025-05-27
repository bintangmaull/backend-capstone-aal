# app/repository/repo_visualisasi_directloss.py

from sqlalchemy import text
from app.extensions import db
import logging
import sys
import os

# UTF-8 for console/logging
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# Setup debug output directory & logger
DEBUG_DIR = os.path.join(os.getcwd(), "debug_output")
os.makedirs(DEBUG_DIR, exist_ok=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(os.path.join(DEBUG_DIR, "repo_visualisasi_directloss.log"), encoding='utf-8')
fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
logger.addHandler(fh)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
logger.addHandler(ch)


class GedungRepository:
    @staticmethod
    def fetch_geojson(bbox=None, prov=None, kota=None):
        where = ["1=1"]
        params = {}

        if bbox:
            west, south, east, north = map(float, bbox.split(","))
            where.append("b.geom && ST_MakeEnvelope(:west,:south,:east,:north,4326)")
            params.update(west=west, south=south, east=east, north=north)

        if prov:
            where.append("TRIM(LOWER(b.provinsi)) = TRIM(LOWER(:provinsi))")
            params["provinsi"] = prov

        if kota:
            where.append("TRIM(LOWER(b.kota)) = TRIM(LOWER(:kota))")
            params["kota"] = kota

        # Ensure provinsi/kota never null in the JSON properties
        sql = f"""
        SELECT json_build_object(
          'type', 'FeatureCollection',
          'features', COALESCE(json_agg(f), '[]'::json)
        ) AS geojson
        FROM (
          SELECT json_build_object(
            'type', 'Feature',
            'geometry', ST_AsGeoJSON(b.geom)::json,
            'properties',
              -- drop geom, provinsi, kota then re-add non-null
              (to_jsonb(b)
                 - 'geom'
                 - 'provinsi'
                 - 'kota'
              )
              || jsonb_build_object(
                   'provinsi', COALESCE(b.provinsi, ''),
                   'kota',    COALESCE(b.kota, '')
                 )
              || to_jsonb(d)
          ) AS f
          FROM bangunan_copy b
          JOIN hasil_proses_directloss d USING(id_bangunan)
          WHERE {" AND ".join(where)}
        ) sub;
        """
        logger.debug("fetch_geojson SQL:\n%s", sql)
        return db.session.execute(text(sql), params).scalar()

    @staticmethod
    def fetch_provinsi():
        sql = """
        SELECT DISTINCT COALESCE(b.provinsi, '')
        FROM hasil_proses_directloss d
        JOIN bangunan_copy b USING (id_bangunan)
        WHERE b.provinsi IS NOT NULL
        ORDER BY b.provinsi
        """
        logger.debug("fetch_provinsi SQL:\n%s", sql)
        rows = db.session.execute(text(sql)).fetchall()
        # filter out any empty strings just in case
        return [r[0] for r in rows if r[0]]

    @staticmethod
    def fetch_kota(provinsi):
        sql = """
        SELECT DISTINCT COALESCE(b.kota, '')
        FROM hasil_proses_directloss d
        JOIN bangunan_copy b USING (id_bangunan)
        WHERE TRIM(LOWER(b.provinsi)) = TRIM(LOWER(:provinsi))
          AND b.kota IS NOT NULL
        ORDER BY b.kota
        """
        logger.debug("fetch_kota SQL:\n%s", sql)
        rows = db.session.execute(text(sql), {"provinsi": provinsi}).fetchall()
        return [r[0] for r in rows if r[0]]

    @staticmethod
    def fetch_aal_geojson(provinsi=None):
        where_clauses = ["1=1"]
        params = {}

        if provinsi:
            where_clauses.append("TRIM(LOWER(hap.provinsi)) = TRIM(LOWER(:provinsi))")
            params["provinsi"] = provinsi

        sql = f"""
        SELECT json_build_object(
          'type',       'FeatureCollection',
          'features',   COALESCE(json_agg(f), '[]'::json)
        ) AS geojson
        FROM (
          SELECT json_build_object(
            'type',     'Feature',
            'geometry', ST_AsGeoJSON(p.geom)::json,
            'properties', to_jsonb(hap)
          ) AS f
          FROM hasil_aal_provinsi hap
          JOIN provinsi p
            ON TRIM(LOWER(p.provinsi)) = TRIM(LOWER(hap.provinsi))
          WHERE {" AND ".join(where_clauses)}
        ) sub;
        """
        logger.debug("fetch_aal_geojson SQL:\n%s", sql)
        return db.session.execute(text(sql), params).scalar()

    @staticmethod
    def fetch_aal_provinsi_list():
        sql = """
        SELECT provinsi
        FROM hasil_aal_provinsi
        WHERE provinsi IS NOT NULL AND provinsi <> ''
        ORDER BY provinsi
        """
        logger.debug("fetch_aal_provinsi_list SQL:\n%s", sql)
        rows = db.session.execute(text(sql)).fetchall()
        return [r[0] for r in rows]

    @staticmethod
    def fetch_aal_data(provinsi):
        sql = """
        SELECT *
        FROM hasil_aal_provinsi
        WHERE TRIM(LOWER(provinsi)) = TRIM(LOWER(:provinsi))
        """
        logger.debug("fetch_aal_data SQL for provinsi=%s", provinsi)
        row = db.session.execute(text(sql), {"provinsi": provinsi}).mappings().first()
        return dict(row) if row else None

    @staticmethod
    def stream_directloss_csv():
        copy_sql = """
        COPY (
          SELECT
            b.id_bangunan,
            COALESCE(b.provinsi,'') AS provinsi,
            COALESCE(b.kota,'')    AS kota,
            d.direct_loss
          FROM bangunan_copy b
          JOIN hasil_proses_directloss d USING (id_bangunan)
        ) TO STDOUT WITH CSV HEADER
        """
        raw_conn = db.session.connection().connection
        cur = raw_conn.cursor()
        logger.debug("stream_directloss_csv copy_sql prepared")
        return cur, copy_sql, {}

    @staticmethod
    def stream_aal_csv():
        copy_sql = """
        COPY (
          SELECT *
          FROM hasil_aal_provinsi
          ORDER BY provinsi
        ) TO STDOUT WITH CSV HEADER
        """
        raw_conn = db.session.connection().connection
        cur = raw_conn.cursor()
        logger.debug("stream_aal_csv copy_sql prepared")
        return cur, copy_sql, {}
