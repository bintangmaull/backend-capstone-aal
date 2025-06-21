# service_directloss.py

import os
import sys
import math
import numpy as np
import pandas as pd
import logging

from sqlalchemy import text 
from app.extensions import db
from app.models.models_database import HasilProsesDirectLoss, HasilAALProvinsi
from app.repository.repo_directloss import get_bangunan_data, get_all_disaster_data, get_db_connection

# UTF-8 for console/logging
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# Setup logger
DEBUG_DIR = os.path.join(os.getcwd(), "debug_output")
os.makedirs(DEBUG_DIR, exist_ok=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(os.path.join(DEBUG_DIR, "service_directloss.log"), encoding="utf-8")
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(formatter)
logger.addHandler(sh)

def process_all_disasters():
    logger.debug("=== START process_all_disasters ===")

    # Clear old
    try:
        db.session.query(HasilProsesDirectLoss).delete()
        db.session.query(HasilAALProvinsi).delete()
        db.session.commit()
        logger.debug("✅ Cleared DirectLoss & AAL")
    except Exception as e:
        db.session.rollback()
        logger.error(f"❌ Clearing old failed: {e}")

    # 1) Building data (with integer jumlah_lantai)
    bld = get_bangunan_data()
    logger.debug(f"📥 Buildings: {len(bld)} rows")
    if 'kode_bangunan' not in bld.columns or bld['kode_bangunan'].isna().all():
        bld['kode_bangunan'] = (
            bld['id_bangunan'].astype(str)
               .str.split('_').str[0]
               .str.lower()
        )
        logger.debug("🔧 Derived kode_bangunan from id_bangunan")
    bld['jumlah_lantai'] = bld['jumlah_lantai'].fillna(0).astype(int)
    bld['luas'] = bld['luas'].fillna(0)
    bld['hsbgn'] = bld['hsbgn'].fillna(0)

    coeff_map = {
        1: 1.000, 2: 1.090, 3: 1.120, 4: 1.135,
        5: 1.162, 6: 1.197, 7: 1.236, 8: 1.265,
    }
    floors_clipped = bld['jumlah_lantai'].clip(1, 8).astype(int)
    bld['hsbgn_coeff']     = floors_clipped.map(coeff_map).fillna(1.0)
    bld['adjusted_hsbgn']  = bld['hsbgn'] * bld['hsbgn_coeff']

    luas    = bld['luas'].to_numpy()
    hsbgn   = bld['adjusted_hsbgn'].to_numpy()

    # 2) Hazard data (reindexed to bld.index!)
    disaster_data = get_all_disaster_data()
    for name, df in disaster_data.items():
        # fill na, then reindex so length==len(bld)
        df = (
            df
            .set_index('id_bangunan')                  # pakai id_bangunan sebagai index
            .reindex(bld['id_bangunan'], fill_value=0) # selaraskan berdasarkan id_bangunan
            .reset_index(drop=True)                    # kembalikan index default agar 1-1 dengan bld
        )
        disaster_data[name] = df
        logger.debug(f"📥 {name}: {len(df)} rows (aligned to {len(bld)})")

    # 3) Direct loss calc
    prefix_map = {"gempa":"mmi","banjir":"depth","longsor":"mflux","gunungberapi":"kpa"}
    scales_map = {
      "gempa": ["500","250","100"],
      "banjir": ["100","50","25"],
      "longsor": ["5","2"],
      "gunungberapi": ["250","100","50"]
    }

    for name, df_raw in disaster_data.items():
        pre    = prefix_map[name]
        scales = scales_map[name]
        if name == "banjir":
            floors = np.clip(bld['jumlah_lantai'].to_numpy(), 1, 2)
            for s in scales:
                y1 = df_raw[f"nilai_y_1_{pre}{s}"].to_numpy()
                y2 = df_raw[f"nilai_y_2_{pre}{s}"].to_numpy()
                v = np.where(floors == 1, y1, y2)
                col = f"direct_loss_{name}_{s}"
                bld[col] = luas * hsbgn * v
                bld[col] = bld[col].fillna(0)
                logger.debug(f"{col} sample: {bld[col].head(3).tolist()}")
        else:
            for s in scales:
                # Mengambil kolom berdasarkan return period dan skala untuk gempa, longsor, dan gunung berapi
                damage_ratio_col = f"nilai_y_cr_{pre}{s}"  # Misalnya untuk gempa pada skala tertentu
                if name == "longsor":
                    damage_ratio_col = f"nilai_y_mur_{pre}{s}"  # Sesuaikan dengan longsor
                elif name == "gunungberapi":
                    damage_ratio_col = f"nilai_y_lightwood_{pre}{s}"  # Sesuaikan dengan gunung berapi

                damage_ratio = df_raw[damage_ratio_col].to_numpy()
                col = f"direct_loss_{name}_{s}"
                bld[col] = luas * hsbgn * damage_ratio
                bld[col] = bld[col].fillna(0)
                logger.debug(f"{col} sample: {bld[col].head(3).tolist()}")

    # 4) Save Direct Loss
    dl_cols = [c for c in bld.columns if c.startswith("direct_loss_")]

    bld = bld.drop_duplicates(subset='id_bangunan', keep='last')    

    mappings = [
        {"id_bangunan": row['id_bangunan'], **{c: row[c] for c in dl_cols}}
        for _, row in bld.iterrows()
    ]
    try:
        db.session.bulk_insert_mappings(HasilProsesDirectLoss, mappings)
        db.session.commit()
        logger.info("✅ Direct Loss saved")
    except Exception as e:
        db.session.rollback()
        logger.error(f"❌ Saving Direct Loss failed: {e}")
        raise

    # 5) Dump CSV & 6) AAL
    csv_path = os.path.join(DEBUG_DIR, "directloss_all.csv")
    cols_to_dump = ["provinsi", "kode_bangunan"] + dl_cols
    bld.to_csv(csv_path, index=False, sep=';', columns=cols_to_dump)
    logger.debug(f"📄 CSV DirectLoss subset for AAL: {csv_path}")

    calculate_aal()
    logger.debug("=== END process_all_disasters ===")
    return csv_path

def calculate_aal():
    path = os.path.join(DEBUG_DIR, "directloss_all.csv")
    if not os.path.exists(path):
        logger.error("❌ directloss_all.csv not found")
        return

    df = pd.read_csv(path, delimiter=';').fillna(0)

    periods = {
      "gempa_500":0.002, "gempa_250":0.004, "gempa_100":0.010,
      "banjir_100":0.01,"banjir_50":0.02,"banjir_25":0.04,
      "gunungberapi_250":0.004,"gunungberapi_100":0.01,"gunungberapi_50":0.02,
      "longsor_5":0.2,"longsor_2":0.5
    }

    dl_cols = [c for c in df.columns if c.startswith("direct_loss_")]
    grp = df.groupby(["provinsi", "kode_bangunan"]).sum()[dl_cols]
    logger.debug(f"grp (provinsi,kode_bangunan) shape: {grp.shape}")

    aal = pd.DataFrame(index=grp.index)
    for key, p in periods.items():
        dis, sc = key.split("_")
        dlc = f"direct_loss_{dis}_{sc}"
        aalc = f"aal_{dis}_{sc}"
        aal[aalc] = grp[dlc] * p
    aal.reset_index(inplace=True)
    aal = aal.fillna(0)
    logger.debug(f"AAL before pivot: {aal.shape}")

    pivot = aal.pivot(index='provinsi', columns='kode_bangunan')
    pivot.columns = [f"{col[0]}_{col[1].lower()}" for col in pivot.columns]
    pivot.reset_index(inplace=True)
    pivot = pivot.fillna(0)
    logger.debug(f"pivot shape: {pivot.shape}")

    for key in periods.keys():
        pattern = f"aal_{key}_"
        cols = [c for c in pivot.columns if c.startswith(pattern) and not c.endswith("_total")]
        pivot[f"{pattern}total"] = pivot[cols].sum(axis=1)
    pivot = pivot.fillna(0)
    logger.debug(f"pivot with totals shape: {pivot.shape}")

    totals = pivot.select_dtypes(include=[np.number]).sum().to_dict()
    totals["provinsi"] = "Total Keseluruhan"
    final = pd.concat([pivot, pd.DataFrame([totals])], ignore_index=True).fillna(0)

    out = os.path.join(DEBUG_DIR, "AAL_per_provinsi_filtered.csv")
    final.to_csv(out, index=False, sep=';')
    logger.debug(f"📄 CSV AAL: {out}")

    try:
        db.session.query(HasilAALProvinsi).delete()
        db.session.bulk_insert_mappings(HasilAALProvinsi, final.to_dict('records'))
        db.session.commit()
        logger.info("✅ AAL saved")
    except Exception as e:
        db.session.rollback()
        logger.error(f"❌ Saving AAL failed: {e}")

def recalc_building_directloss_and_aal(bangunan_id: str):
    logger.debug(f"=== START incremental recalc for {bangunan_id} ===")

    # Ambil data bangunan
    engine = get_db_connection()
    with engine.connect() as conn:
        b_query = text("""
            SELECT
              b.geom,
              b.luas,
              COALESCE(k.hsbgn, 0) AS hsbgn,
              COALESCE(b.jumlah_lantai, 0) AS jumlah_lantai,
              b.provinsi,
              LOWER(split_part(b.id_bangunan, '_', 1)) AS kode_bangunan
            FROM bangunan_copy b
            LEFT JOIN kota k ON b.kota = k.kota
            WHERE b.id_bangunan = :id
        """)
        b = conn.execute(b_query, {"id": bangunan_id}).mappings().first()
        if not b:
            raise ValueError(f"Bangunan {bangunan_id} tidak ditemukan")

        geom       = b["geom"]
        luas_val   = b["luas"]
        hsbgn_val_raw = b["hsbgn"]
        raw_floors    = int(b["jumlah_lantai"])

        # lantai banjir 
        floor_banjir   = int(np.clip(raw_floors, 1, 2))
        # clip lantai antara 1–8
        floor_hsbgn    = int(np.clip(raw_floors, 1, 8))

        # peta koefisien HSBGN per lantai
        coeff_map = {
            1: 1.000,
            2: 1.090,
            3: 1.120,
            4: 1.135,
            5: 1.162,
            6: 1.197,
            7: 1.236,
            8: 1.265,
        }

        # hitung adjusted hsbgn
        hsbgn_val = hsbgn_val_raw * coeff_map.get(floor_hsbgn, 1.0)

        prov       = b["provinsi"]
        kode_bgn   = b["kode_bangunan"]

        mapping = {
            "gempa": {
                "raw":      "model_intensitas_gempa",
                "dmgr":     "dmgratio_gempa",
                "prefix":   "mmi",
                "scales":   ["500","250","100"],
                "threshold": 9500,
                "vcols":    lambda pre,s: [
                    f"h.dmgratio_cr_{pre}{s}         AS nilai_y_cr_{pre}{s}",
                    f"h.dmgratio_mcf_{pre}{s}        AS nilai_y_mcf_{pre}{s}",
                    f"h.dmgratio_mur_{pre}{s}        AS nilai_y_mur_{pre}{s}",
                    f"h.dmgratio_lightwood_{pre}{s}  AS nilai_y_lightwood_{pre}{s}",
                ]
            },
            "banjir": {
                "raw":      "model_intensitas_banjir",
                "dmgr":     "dmgratio_banjir_copy",
                "prefix":   "depth",
                "scales":   ["100","50","25"],
                "threshold": 700,
                "vcols":    lambda pre,s: [
                    f"h.dmgratio_1_{pre}{s} AS nilai_y_1_{pre}{s}",
                    f"h.dmgratio_2_{pre}{s} AS nilai_y_2_{pre}{s}",
                ]
            },
            "longsor": {
                "raw":      "model_intensitas_longsor",
                "dmgr":     "dmgratio_longsor",
                "prefix":   "mflux",
                "scales":   ["5","2"],
                "threshold": 700,
                "vcols":    lambda pre,s: [
                    f"h.dmgratio_cr_{pre}{s}         AS nilai_y_cr_{pre}{s}",
                    f"h.dmgratio_mcf_{pre}{s}        AS nilai_y_mcf_{pre}{s}",
                    f"h.dmgratio_mur_{pre}{s}        AS nilai_y_mur_{pre}{s}",
                    f"h.dmgratio_lightwood_{pre}{s}  AS nilai_y_lightwood_{pre}{s}",
                ]
            },
            "gunungberapi": {
                "raw":      "model_intensitas_gunungberapi",
                "dmgr":     "dmgratio_gunungberapi",
                "prefix":   "kpa",
                "scales":   ["250","100","50"],
                "threshold": 550,
                "vcols":    lambda pre,s: [
                    f"h.dmgratio_cr_{pre}{s}         AS nilai_y_cr_{pre}{s}",
                    f"h.dmgratio_mcf_{pre}{s}        AS nilai_y_mcf_{pre}{s}",
                    f"h.dmgratio_mur_{pre}{s}        AS nilai_y_mur_{pre}{s}",
                    f"h.dmgratio_lightwood_{pre}{s}  AS nilai_y_lightwood_{pre}{s}",
                ]
            }
        }

        # 3) Hitung direct_loss per jenis & skala hanya untuk bangunan ini
        direct_losses = {}
        for nama, cfg in mapping.items():
            raw_table  = cfg["raw"]
            dmgr_table = cfg["dmgr"]
            pre        = cfg["prefix"]
            scales     = cfg["scales"]
            thr        = cfg["threshold"]
            vcols_fn   = cfg["vcols"]

            # build subquery cols & outer cols
            subq_parts = []
            outer_cols = []
            for s in scales:
                for expr in vcols_fn(pre, s):
                    subq_parts.append(expr)
                    alias = expr.split(" AS ")[1]
                    outer_cols.append(f"near.{alias}")

            subq_sql  = ", ".join(subq_parts)
            outer_sql = ", ".join(outer_cols)

            sql = text(f"""
                SELECT {outer_sql}
                FROM bangunan_copy b
                JOIN LATERAL (
                  SELECT {subq_sql}
                  FROM {raw_table} r
                  JOIN {dmgr_table} h USING(id_lokasi)
                  WHERE ST_DWithin(b.geom::geography, r.geom::geography, {thr})
                  ORDER BY b.geom::geography <-> r.geom::geography
                  LIMIT 1
                ) AS near ON TRUE
                WHERE b.id_bangunan = :id
            """)

            near = conn.execute(sql, {"id": bangunan_id}).mappings().first() or {}

            for s in scales:
                dlc = f"direct_loss_{nama}_{s}"
                if nama == "banjir":
                    y1 = near.get(f"nilai_y_1_{pre}{s}", 0)
                    y2 = near.get(f"nilai_y_2_{pre}{s}", 0)
                    v  = y1 if floor_banjir == 1 else y2
                else:
                    # Sesuaikan perhitungan dengan return period
                    damage_ratio_col = f"nilai_y_cr_{pre}{s}"  # Misalnya untuk gempa pada skala tertentu
                    if nama == "longsor":
                        damage_ratio_col = f"nilai_y_mur_{pre}{s}"  # Sesuaikan dengan longsor
                    elif nama == "gunungberapi":
                        damage_ratio_col = f"nilai_y_lightwood_{pre}{s}"  # Sesuaikan dengan gunung berapi
                    v = near.get(damage_ratio_col, 0)

                if v is None or (isinstance(v, float) and math.isnan(v)):
                    v = 0.0
                direct_losses[dlc] = float(luas_val * hsbgn_val * v)

    # 4) Simpan DirectLoss & update AAL seperti semula
    old = db.session.query(HasilProsesDirectLoss).filter_by(id_bangunan=bangunan_id).one_or_none()
    old_vals = {c: getattr(old, c) for c in direct_losses} if old else {c: 0 for c in direct_losses}

    if old:
        db.session.delete(old)
        db.session.commit()
    new_rec = HasilProsesDirectLoss(id_bangunan=bangunan_id, **direct_losses)
    db.session.add(new_rec)
    db.session.commit()
    logger.debug(f"✅ DirectLoss updated for {bangunan_id}")

    periods = {
      "gempa_500":0.002, "gempa_250":0.004, "gempa_100":0.010,
      "banjir_100":0.01,"banjir_50":0.02,"banjir_25":0.04,
      "gunungberapi_250":0.004,"gunungberapi_100":0.01,"gunungberapi_50":0.02,
      "longsor_5":0.2,"longsor_2":0.5
    }

    aal_row = db.session.query(HasilAALProvinsi).filter_by(provinsi=prov).one_or_none()
    if not aal_row:
        raise RuntimeError(f"AALProvinsi untuk '{prov}' tidak ditemukan")

    for key, p in periods.items():
        dis, sc = key.split("_")
        dlc = f"direct_loss_{dis}_{sc}"
        delta = direct_losses[dlc] - old_vals.get(dlc, 0)
        delta_aal = float(delta * p)
        col_tax = f"aal_{dis}_{sc}_{kode_bgn}"
        col_tot = f"aal_{dis}_{sc}_total"
        # update atribut objek dan juga via UPDATE
        setattr(aal_row, col_tax, float(getattr(aal_row, col_tax, 0)) + delta_aal)
        setattr(aal_row, col_tot, float(getattr(aal_row, col_tot, 0)) + delta_aal)
        db.session.query(HasilAALProvinsi)\
            .filter_by(provinsi=prov)\
            .update({
                col_tax: HasilAALProvinsi.__table__.c[col_tax] + delta_aal,
                col_tot: HasilAALProvinsi.__table__.c[col_tot] + delta_aal
            })

    db.session.commit()
    logger.info(f"✅ AAL incremental updated for provinsi {prov}")
    logger.debug(f"=== END incremental recalc for {bangunan_id} ===")

    return {"direct_losses": direct_losses}
