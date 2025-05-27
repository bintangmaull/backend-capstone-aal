import math
import time
import random
import io
import csv

import pandas as pd
from app.extensions import db
from app.repository.repo_crud_bangunan import BangunanRepository
from app.models.models_database import HasilProsesDirectLoss, HasilAALProvinsi
from app.repository.repo_directloss import get_bangunan_data
from app.service.service_directloss import recalc_building_directloss_and_aal  # << import baru

class BangunanService:
    @staticmethod
    def get_all_bangunan(provinsi=None, kota=None, nama=None):
        return BangunanRepository.get_all(provinsi, kota, nama)

    @staticmethod
    def get_bangunan_by_id(bangunan_id):
        return BangunanRepository.get_by_id(bangunan_id)

    @staticmethod
    def create_bangunan(data):
        return BangunanRepository.create(data)

    @staticmethod
    def update_bangunan(bangunan_id, data):
        return BangunanRepository.update(bangunan_id, data)

    @staticmethod
    def delete_bangunan(bangunan_id, prov):
        kode_bgn = (
                bangunan_id.split('_')[0].lower()
        )
        old = db.session.query(HasilProsesDirectLoss).filter_by(id_bangunan=bangunan_id).one_or_none()
        dl_cols = [
            "direct_loss_gempa_500",
            "direct_loss_gempa_250",
            "direct_loss_gempa_100",
            "direct_loss_banjir_100",
            "direct_loss_banjir_50",
            "direct_loss_banjir_25",
            "direct_loss_longsor_5",
            "direct_loss_longsor_2",
            "direct_loss_gunungberapi_250",
            "direct_loss_gunungberapi_100",
            "direct_loss_gunungberapi_50",
        ]

        # 3) Build old_vals as a dict of { column_name: value }
        if old:
            old_vals = { col: getattr(old, col) or 0.0 for col in dl_cols }
        else:
            # if there's no existing record, default everything to 0
            old_vals = { col: 0.0 for col in dl_cols }
        if old:
            db.session.delete(old)
            db.session.commit()
        BangunanRepository.delete(bangunan_id)
        periods = {
        "gempa_500":0.02,"gempa_250":0.04,"gempa_100":0.10,
        "banjir_100":0.05,"banjir_50":0.10,"banjir_25":0.20,
        "gunungberapi_250":0.01,"gunungberapi_100":0.03,"gunungberapi_50":0.05,
        "longsor_5":0.02,"longsor_2":0.04
        }

        aal_row = db.session.query(HasilAALProvinsi)\
                    .filter_by(provinsi=prov).one_or_none()
        if not aal_row:
            raise RuntimeError(f"AALProvinsi untuk '{prov}' tidak ditemukan")

        for key,p in periods.items():
            dis,sc = key.split("_")
            dlc = f"direct_loss_{dis}_{sc}"
            delta  = - old_vals.get(dlc,0)
            delta_aal = float(delta * (-math.log(1-p)))
            col_tax = f"aal_{dis}_{sc}_{kode_bgn}"
            col_tot = f"aal_{dis}_{sc}_total"
            setattr(aal_row,col_tax, float(getattr(aal_row,col_tax,0)+delta_aal))
            setattr(aal_row,col_tot, float(getattr(aal_row,col_tot,0)+delta_aal))
        db.session.commit()
        return {"deleted": "The Bangunan HAs deleted"}


    @staticmethod
    def generate_unique_id(taxonomy: str) -> str:
        if taxonomy not in ("BMN", "FS", "FD"):
            raise ValueError("kode_bangunan invalid, harus BMN/FS/FD")
        while True:
            ts = int(time.time())
            suffix = random.randint(100, 999)
            candidate = f"{taxonomy}_{ts}{suffix}"
            if not BangunanRepository.exists_id(candidate):
                return candidate

    @staticmethod
    def get_provinsi_list():
        return BangunanRepository.get_provinsi_list()

    @staticmethod
    def get_kota_list(provinsi):
        return BangunanRepository.get_kota_list(provinsi)

    @staticmethod
    def upload_csv(file_storage):
        """
        Baca CSV dengan kolom:
          nama_gedung, alamat, provinsi, kota,
          lon, lat,
          kode_bangunan (BMN/FS/FD),
          taxonomy (MUR/MCF/CR/Light Wood),
          luas
        Generate id_bangunan per baris dari kode_bangunan,
        lalu insert tanpa geom (Postgres akan generate geom).
        """
        text = file_storage.stream.read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))
        created = 0

        for row in reader:
            # trim all inputs
            nama     = row.get("nama_gedung","").strip()
            alamat   = row.get("alamat","").strip()
            prov     = row.get("provinsi","").strip()
            kota     = row.get("kota","").strip()
            kode     = row.get("kode_bangunan","").strip()   # BMN/FS/FD
            tax      = row.get("taxonomy","").strip()        # MUR/MCF/CR/Light Wood
            lon      = float(row.get("lon") or 0)
            lat      = float(row.get("lat") or 0)
            luas     = float(row.get("luas") or 0)

            # generate id from kode_bangunan, not taxonomy
            if kode not in ("BMN","FS","FD"):
                raise ValueError(f"Invalid kode_bangunan '{kode}' at line {reader.line_num}")
            data = {
                "id_bangunan": BangunanService.generate_unique_id(kode),
                "nama_gedung": nama,
                "alamat":      alamat,
                "provinsi":    prov,
                "kota":        kota,
                "lon":         lon,
                "lat":         lat,
                "taxonomy":    tax,
                "luas":        luas
            }

            # insert record (geom akan di-generate di Postgres)
            BangunanRepository.create(data)
            created += 1

        return {"created": created}

    # ====================================================================
    # Metode baru: recalc Direct Loss & AAL untuk satu bangunan spesifik
    # ====================================================================
    @staticmethod
    def recalc_building_directloss_and_aal(bangunan_id: str):
        """
        Pertama periksa eksistensi bangunan di DB.
        Jika ada, delegasikan ke service_directloss.recalc_building_directloss_and_aal.
        """
        if not BangunanRepository.exists_id(bangunan_id):
            # kembalikan HTTP 400 via controller dengan ValueError di-raise
            raise ValueError(f"Bangunan '{bangunan_id}' tidak ditemukan")
        # panggil service_directloss yang melakukan perhitungan ulang
        return recalc_building_directloss_and_aal(bangunan_id)
