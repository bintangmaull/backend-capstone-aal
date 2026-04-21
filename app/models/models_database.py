from app.extensions import db
from geoalchemy2 import Geometry

# === Raw Data Tables (Input) ===

class RawGempa(db.Model):
    __tablename__ = 'model_intensitas_gempa'
    id_lokasi = db.Column(db.Integer, primary_key=True)
    lon = db.Column(db.Float)
    lat = db.Column(db.Float)
    pga_100  = db.Column(db.Float)
    pga_200  = db.Column(db.Float)
    pga_250  = db.Column(db.Float)
    pga_500  = db.Column(db.Float)
    pga_1000 = db.Column(db.Float)
    geom = db.Column(Geometry('POINT', srid=4326))

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


class RawTsunami(db.Model):
    __tablename__ = 'model_intensitas_tsunami'
    id_lokasi  = db.Column(db.Integer, primary_key=True)
    lon        = db.Column(db.Float)
    lat        = db.Column(db.Float)
    inundansi  = db.Column(db.Float)
    geom       = db.Column(Geometry('POINT', srid=4326))

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


class RawBanjir(db.Model):
    __tablename__ = 'model_intensitas_banjir'
    id_lokasi = db.Column(db.Integer, primary_key=True)
    lon       = db.Column(db.Float)
    lat       = db.Column(db.Float)
    r_2       = db.Column(db.Float)
    r_5       = db.Column(db.Float)
    r_10      = db.Column(db.Float)
    r_25      = db.Column(db.Float)
    r_50      = db.Column(db.Float)
    r_100     = db.Column(db.Float)
    r_250     = db.Column(db.Float)
    rc_2      = db.Column('rc_2', db.Float)
    rc_5      = db.Column('rc_5', db.Float)
    rc_10     = db.Column('rc_10', db.Float)
    rc_25     = db.Column('rc_25', db.Float)
    rc_50     = db.Column('rc_50', db.Float)
    rc_100    = db.Column('rc_100', db.Float)
    rc_250    = db.Column('rc_250', db.Float)
    geom      = db.Column(Geometry('POINT', srid=4326))

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}

# Aliases for backward compatibility in codebase
RawBanjirR = RawBanjir
RawBanjirRC = RawBanjir


# === Processed Damage Ratio Tables ===

class HasilProsesGempa(db.Model):
    __tablename__ = 'dmg_ratio_gempa'

    id_lokasi = db.Column(db.Integer, primary_key=True, autoincrement=True)

    # PGA 100
    dmgratio_cr_pga100   = db.Column(db.Float, nullable=True)
    dmgratio_mcf_pga100  = db.Column(db.Float, nullable=True)
    # PGA 200
    dmgratio_cr_pga200   = db.Column(db.Float, nullable=True)
    dmgratio_mcf_pga200  = db.Column(db.Float, nullable=True)
    # PGA 250
    dmgratio_cr_pga250   = db.Column(db.Float, nullable=True)
    dmgratio_mcf_pga250  = db.Column(db.Float, nullable=True)
    # PGA 500
    dmgratio_cr_pga500   = db.Column(db.Float, nullable=True)
    dmgratio_mcf_pga500  = db.Column(db.Float, nullable=True)
    # PGA 1000
    dmgratio_cr_pga1000  = db.Column(db.Float, nullable=True)
    dmgratio_mcf_pga1000 = db.Column(db.Float, nullable=True)

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


class HasilProsesTsunami(db.Model):
    __tablename__ = 'dmg_ratio_tsunami'

    id_lokasi             = db.Column(db.Integer, primary_key=True, autoincrement=True)
    dmgratio_cr_inundansi  = db.Column(db.Float, nullable=True)
    dmgratio_mcf_inundansi = db.Column(db.Float, nullable=True)

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


class HasilProsesBanjir(db.Model):
    __tablename__ = 'dmg_ratio_banjir'

    id_lokasi         = db.Column(db.Integer, primary_key=True, autoincrement=True)
    # R-2, 5, 10, 25, 50, 100, 250
    dmgratio_1_r2     = db.Column(db.Float, nullable=True, default=0)
    dmgratio_2_r2     = db.Column(db.Float, nullable=True, default=0)
    dmgratio_1_r5     = db.Column(db.Float, nullable=True, default=0)
    dmgratio_2_r5     = db.Column(db.Float, nullable=True, default=0)
    dmgratio_1_r10    = db.Column(db.Float, nullable=True, default=0)
    dmgratio_2_r10    = db.Column(db.Float, nullable=True, default=0)
    dmgratio_1_r25    = db.Column(db.Float, nullable=True, default=0)
    dmgratio_2_r25    = db.Column(db.Float, nullable=True, default=0)
    dmgratio_1_r50    = db.Column(db.Float, nullable=True, default=0)
    dmgratio_2_r50    = db.Column(db.Float, nullable=True, default=0)
    dmgratio_1_r100   = db.Column(db.Float, nullable=True, default=0)
    dmgratio_2_r100   = db.Column(db.Float, nullable=True, default=0)
    dmgratio_1_r250   = db.Column(db.Float, nullable=True, default=0)
    dmgratio_2_r250   = db.Column(db.Float, nullable=True, default=0)
    # RC-2, 5, 10, 25, 50, 100, 250
    dmgratio_1_rc2    = db.Column(db.Float, nullable=True, default=0)
    dmgratio_2_rc2    = db.Column(db.Float, nullable=True, default=0)
    dmgratio_1_rc5    = db.Column(db.Float, nullable=True, default=0)
    dmgratio_2_rc5    = db.Column(db.Float, nullable=True, default=0)
    dmgratio_1_rc10   = db.Column(db.Float, nullable=True, default=0)
    dmgratio_2_rc10   = db.Column(db.Float, nullable=True, default=0)
    dmgratio_1_rc25   = db.Column(db.Float, nullable=True, default=0)
    dmgratio_2_rc25   = db.Column(db.Float, nullable=True, default=0)
    dmgratio_1_rc50   = db.Column(db.Float, nullable=True, default=0)
    dmgratio_2_rc50   = db.Column(db.Float, nullable=True, default=0)
    dmgratio_1_rc100  = db.Column(db.Float, nullable=True, default=0)
    dmgratio_2_rc100  = db.Column(db.Float, nullable=True, default=0)
    dmgratio_1_rc250  = db.Column(db.Float, nullable=True, default=0)
    dmgratio_2_rc250  = db.Column(db.Float, nullable=True, default=0)

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}

# Aliases for backward compatibility
HasilProsesBanjirR = HasilProsesBanjir
HasilProsesBanjirRC = HasilProsesBanjir


# === Data HSBGN ===
class HSBGN(db.Model):
    __tablename__ = 'kota'

    id_kota  = db.Column(db.Integer, primary_key=True, autoincrement=True)
    kota     = db.Column(db.String(255), nullable=False, unique=True)
    provinsi = db.Column(db.String(255), nullable=False, unique=True)
    hsbgn_sederhana      = db.Column(db.Float, nullable=False)
    hsbgn_tidaksederhana = db.Column(db.Float, nullable=False)

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


class Bangunan(db.Model):
    __tablename__ = "bangunan_copy"

    id_bangunan   = db.Column(db.String(50), primary_key=True)
    lon           = db.Column(db.Float, nullable=False)
    lat           = db.Column(db.Float, nullable=False)
    taxonomy      = db.Column(db.String(100), nullable=False)
    luas          = db.Column(db.Float, nullable=False)
    nama_gedung   = db.Column(db.String(255))
    alamat        = db.Column(db.String(255))
    kota          = db.Column(db.String(255), nullable=False)
    provinsi      = db.Column(db.String(255), nullable=False)
    geom          = db.Column(Geometry('POINT', srid=4326), nullable=False)
    jumlah_lantai = db.Column(db.Integer)
    kode_bangunan = db.Column(db.String(255))

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


class ExposureBMNResidential(db.Model):
    __tablename__ = "exposure_bmn_residential"

    id            = db.Column(db.String(50), primary_key=True)
    lon           = db.Column(db.Float, nullable=False)
    lat           = db.Column(db.Float, nullable=False)
    taxonomy      = db.Column(db.String(100))
    jumlah_lantai = db.Column(db.Integer)
    nama_gedung   = db.Column(db.String(255))
    luas          = db.Column(db.Float)
    nilai_aset    = db.Column(db.Float)
    kota          = db.Column(db.String(255))
    provinsi      = db.Column(db.String(255))
    geom          = db.Column(Geometry('POINT', srid=4326), nullable=False)

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


# === Provinsi ===
class Provinsi(db.Model):
    __tablename__ = 'provinsi'
    id_provinsi = db.Column(db.Integer, primary_key=True)
    provinsi    = db.Column(db.String(255), nullable=False, unique=True)
    geom        = db.Column(Geometry('MULTIPOLYGON', srid=4326))

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


# === Hasil Proses Direct Loss ===
class HasilProsesDirectLoss(db.Model):
    __tablename__ = 'hasil_proses_directloss'

    id_bangunan = db.Column(db.String, primary_key=True, nullable=False)

    # Gempa (PGA)
    direct_loss_pga_100  = db.Column(db.Float, default=0)
    direct_loss_pga_200  = db.Column(db.Float, default=0)
    direct_loss_pga_250  = db.Column(db.Float, default=0)
    direct_loss_pga_500  = db.Column(db.Float, default=0)
    direct_loss_pga_1000 = db.Column(db.Float, default=0)

    # Tsunami
    direct_loss_inundansi = db.Column(db.Float, default=0)

    # Banjir R
    direct_loss_r_2   = db.Column(db.Float, default=0)
    direct_loss_r_5   = db.Column(db.Float, default=0)
    direct_loss_r_10  = db.Column(db.Float, default=0)
    direct_loss_r_25  = db.Column(db.Float, default=0)
    direct_loss_r_50  = db.Column(db.Float, default=0)
    direct_loss_r_100 = db.Column(db.Float, default=0)
    direct_loss_r_250 = db.Column(db.Float, default=0)

    # Banjir RC
    direct_loss_rc_2   = db.Column(db.Float, default=0)
    direct_loss_rc_5   = db.Column(db.Float, default=0)
    direct_loss_rc_10  = db.Column(db.Float, default=0)
    direct_loss_rc_25  = db.Column(db.Float, default=0)
    direct_loss_rc_50  = db.Column(db.Float, default=0)
    direct_loss_rc_100 = db.Column(db.Float, default=0)
    direct_loss_rc_250 = db.Column(db.Float, default=0)

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


# === Hasil AAL per Kota ===
class HasilAALProvinsi(db.Model):
    __tablename__ = 'hasil_aal_kota'
    id_kota = db.Column(db.String(255), primary_key=True, nullable=False, unique=True)

    # --- FS ---
    aal_pga_fs         = db.Column(db.Float, default=0)
    aal_inundansi_fs   = db.Column(db.Float, default=0)
    aal_r_fs           = db.Column(db.Float, default=0)
    aal_rc_fs          = db.Column(db.Float, default=0)

    # --- FD ---
    aal_pga_fd         = db.Column(db.Float, default=0)
    aal_inundansi_fd   = db.Column(db.Float, default=0)
    aal_r_fd           = db.Column(db.Float, default=0)
    aal_rc_fd          = db.Column(db.Float, default=0)

    # --- BMN ---
    aal_pga_bmn         = db.Column(db.Float, default=0)
    aal_inundansi_bmn   = db.Column(db.Float, default=0)
    aal_r_bmn           = db.Column(db.Float, default=0)
    aal_rc_bmn          = db.Column(db.Float, default=0)

    # --- Residential (res) ---
    aal_pga_res         = db.Column(db.Float, default=0)
    aal_inundansi_res   = db.Column(db.Float, default=0)
    aal_r_res           = db.Column(db.Float, default=0)
    aal_rc_res          = db.Column(db.Float, default=0)

    # --- Electricity ---
    aal_pga_electricity       = db.Column(db.Float, default=0)
    aal_inundansi_electricity = db.Column(db.Float, default=0)
    aal_r_electricity         = db.Column(db.Float, default=0)
    aal_rc_electricity        = db.Column(db.Float, default=0)

    # --- Hotel ---
    aal_pga_hotel       = db.Column(db.Float, default=0)
    aal_inundansi_hotel = db.Column(db.Float, default=0)
    aal_r_hotel         = db.Column(db.Float, default=0)
    aal_rc_hotel        = db.Column(db.Float, default=0)

    # --- Airport ---
    aal_pga_airport       = db.Column(db.Float, default=0)
    aal_inundansi_airport = db.Column(db.Float, default=0)
    aal_r_airport         = db.Column(db.Float, default=0)
    aal_rc_airport        = db.Column(db.Float, default=0)

    # --- Total per bencana ---
    aal_pga_total        = db.Column(db.Float, default=0)
    aal_inundansi_total  = db.Column(db.Float, default=0)
    aal_r_total          = db.Column(db.Float, default=0)
    aal_rc_total         = db.Column(db.Float, default=0)

    # --- Exposure (Total Asset) ---
    hotel               = db.Column(db.Float, default=0)
    res                 = db.Column(db.Float, default=0)
    airport             = db.Column(db.Float, default=0)

    def to_dict(self):
        return {column.name: getattr(self, column.name) for column in self.__table__.columns}


class HasilPMLGempaKota(db.Model):
    __tablename__ = 'hasil_pml_gempa_kota'
    id_kota = db.Column(db.String(100), primary_key=True)
    return_period = db.Column(db.Integer, primary_key=True)
    
    pml_airport = db.Column(db.Float, default=0)
    pml_fd = db.Column(db.Float, default=0)
    pml_electricity = db.Column(db.Float, default=0)
    pml_fs = db.Column(db.Float, default=0)
    pml_hotel = db.Column(db.Float, default=0)
    pml_bmn = db.Column(db.Float, default=0)
    pml_res = db.Column(db.Float, default=0)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


# === Rekap Aset per Kota ===
class RekapAsetKota(db.Model):
    __tablename__ = 'rekap_aset_kota'
    id_kota = db.Column(db.String(255), primary_key=True, nullable=False, unique=True)

    count_fs = db.Column(db.Integer, default=0)
    total_asset_fs = db.Column(db.Float, default=0)

    count_fd = db.Column(db.Integer, default=0)
    total_asset_fd = db.Column(db.Float, default=0)

    count_electricity = db.Column(db.Integer, default=0)
    total_asset_electricity = db.Column(db.Float, default=0)

    count_hotel = db.Column(db.Integer, default=0)
    total_asset_hotel = db.Column(db.Float, default=0)

    count_airport = db.Column(db.Integer, default=0)
    total_asset_airport = db.Column(db.Float, default=0)

    count_total = db.Column(db.Integer, default=0)
    total_asset_total = db.Column(db.Float, default=0)

    dl_exposure = db.Column(db.JSON, nullable=True)

    # --- Direct Loss & Ratio per Return Period ---
    # Gempa
    dl_sum_pga_100 = db.Column(db.Float, default=0)
    ratio_pga_100  = db.Column(db.Float, default=0)
    dl_sum_pga_200 = db.Column(db.Float, default=0)
    ratio_pga_200  = db.Column(db.Float, default=0)
    dl_sum_pga_250 = db.Column(db.Float, default=0)
    ratio_pga_250  = db.Column(db.Float, default=0)
    dl_sum_pga_500 = db.Column(db.Float, default=0)
    ratio_pga_500  = db.Column(db.Float, default=0)
    dl_sum_pga_1000 = db.Column(db.Float, default=0)
    ratio_pga_1000  = db.Column(db.Float, default=0)

    # Tsunami
    dl_sum_inundansi = db.Column(db.Float, default=0)
    ratio_inundansi  = db.Column(db.Float, default=0)

    # Banjir R
    dl_sum_r_2   = db.Column(db.Float, default=0)
    ratio_r_2    = db.Column(db.Float, default=0)
    dl_sum_r_5   = db.Column(db.Float, default=0)
    ratio_r_5    = db.Column(db.Float, default=0)
    dl_sum_r_10  = db.Column(db.Float, default=0)
    ratio_r_10   = db.Column(db.Float, default=0)
    dl_sum_r_25  = db.Column(db.Float, default=0)
    ratio_r_25   = db.Column(db.Float, default=0)
    dl_sum_r_50  = db.Column(db.Float, default=0)
    ratio_r_50   = db.Column(db.Float, default=0)
    dl_sum_r_100 = db.Column(db.Float, default=0)
    ratio_r_100  = db.Column(db.Float, default=0)
    dl_sum_r_250 = db.Column(db.Float, default=0)
    ratio_r_250  = db.Column(db.Float, default=0)

    # Banjir RC
    dl_sum_rc_2   = db.Column(db.Float, default=0)
    ratio_rc_2    = db.Column(db.Float, default=0)
    dl_sum_rc_5   = db.Column(db.Float, default=0)
    ratio_rc_5    = db.Column(db.Float, default=0)
    dl_sum_rc_10  = db.Column(db.Float, default=0)
    ratio_rc_10   = db.Column(db.Float, default=0)
    dl_sum_rc_25  = db.Column(db.Float, default=0)
    ratio_rc_25   = db.Column(db.Float, default=0)
    dl_sum_rc_50  = db.Column(db.Float, default=0)
    ratio_rc_50   = db.Column(db.Float, default=0)
    dl_sum_rc_100 = db.Column(db.Float, default=0)
    ratio_rc_100  = db.Column(db.Float, default=0)
    dl_sum_rc_250 = db.Column(db.Float, default=0)
    ratio_rc_250  = db.Column(db.Float, default=0)

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


# === Kurva Kerentanan (Reference Curves) ===

class GempaReferenceCurve(db.Model):
    __tablename__ = "referensi_dmgratio_gempa"

    id_referensi = db.Column(db.Integer, primary_key=True)
    tipe_kurva   = db.Column(db.String(10), nullable=False)   # cr, mcf, mur, w
    x            = db.Column(db.Float, nullable=False)
    y            = db.Column(db.Float, nullable=False)


class TsunamiReferenceCurve(db.Model):
    __tablename__ = "referensi_dmgratio_tsunami"

    id_referensi = db.Column(db.Integer, primary_key=True)
    tipe_kurva   = db.Column(db.String(10), nullable=False)   # cr, mcf, mur, w
    x            = db.Column(db.Float, nullable=False)
    y            = db.Column(db.Float, nullable=False)


class BanjirReferenceCurve(db.Model):
    """Satu tabel referensi dipakai untuk banjir_r DAN banjir_rc (tipe_kurva: '1' atau '2')."""
    __tablename__ = "referensi_dmgratio_banjir"

    id_referensi = db.Column(db.Integer, primary_key=True)
    tipe_kurva   = db.Column(db.String(10), nullable=False)   # '1' = 1 lantai, '2' = 2+ lantai
    x            = db.Column(db.Float, nullable=False)
    y            = db.Column(db.Float, nullable=False)


# === Autentikasi: User & Activity Log ===

class User(db.Model):
    __tablename__ = 'users'

    id           = db.Column(db.Integer, primary_key=True, autoincrement=True)
    nama         = db.Column(db.String(255), nullable=False)
    email        = db.Column(db.String(255), nullable=False, unique=True)
    password_hash = db.Column(db.String(512), nullable=False)
    # status: 'pending' | 'approved' | 'rejected'
    status       = db.Column(db.String(20), nullable=False, default='pending')
    created_at   = db.Column(db.DateTime, server_default=db.func.now())

    def to_dict(self):
        return {
            'id': self.id,
            'nama': self.nama,
            'email': self.email,
            'status': self.status,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }


class ActivityLog(db.Model):
    __tablename__ = 'activity_log'

    id          = db.Column(db.Integer, primary_key=True, autoincrement=True)
    user_nama   = db.Column(db.String(255), nullable=False)
    user_email  = db.Column(db.String(255), nullable=False)
    # action: 'tambah' | 'edit' | 'hapus' | 'upload_csv'
    action      = db.Column(db.String(50), nullable=False)
    # target: 'bangunan' | 'hsbgn'
    target      = db.Column(db.String(50), nullable=False)
    target_id   = db.Column(db.String(255), nullable=True)
    detail      = db.Column(db.Text, nullable=True)
    timestamp   = db.Column(db.DateTime, server_default=db.func.now())

    def to_dict(self):
        return {
            'id': self.id,
            'user_nama': self.user_nama,
            'user_email': self.user_email,
            'action': self.action,
            'target': self.target,
            'target_id': self.target_id,
            'detail': self.detail,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
        }

class LossRatioGempa(db.Model):
    __tablename__ = 'loss_ratio_gempa'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    kota = db.Column(db.String(255), nullable=False)
    return_period = db.Column(db.Integer, nullable=False)
    airport_loss_ratio = db.Column(db.Float)
    educational_loss_ratio = db.Column(db.Float)
    electricity_loss_ratio = db.Column(db.Float)
    healthcare_loss_ratio = db.Column(db.Float)
    hotel_loss_ratio = db.Column(db.Float)
    residential_loss_ratio = db.Column(db.Float)
    bmn_loss_ratio = db.Column(db.Float)

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


class LossDroughtSawah(db.Model):
    """Drought (Kekeringan) rice field loss values per city, return period, and climate scenario."""
    __tablename__ = 'loss_drought_sawah'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    kota = db.Column(db.String(255), nullable=False)
    return_period = db.Column(db.Integer, nullable=False)
    climate_change = db.Column(db.String(10), nullable=False)  # 'ncc' or 'cc'
    loss_2022_idr = db.Column(db.Float)
    loss_2025_idr = db.Column(db.Float)
    loss_2028_idr = db.Column(db.Float)
    loss_2022_usd = db.Column(db.Float)
    loss_2025_usd = db.Column(db.Float)
    loss_2028_usd = db.Column(db.Float)

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}

class AALDroughtSawah(db.Model):
    """Drought (Kekeringan) rice field AAL values from CSV."""
    __tablename__ = 'aal_drought_sawah'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    year = db.Column(db.Integer, nullable=False)
    climate_change = db.Column(db.String(10), nullable=False) # 'ncc' or 'cc'
    id_kota = db.Column(db.String(100), nullable=False)
    province_name = db.Column(db.String(100))
    cv = db.Column(db.Float)
    aal = db.Column(db.Float)
    var_95 = db.Column(db.Float)
    tvar_95 = db.Column(db.Float)
    var_99 = db.Column(db.Float)
    tvar_99 = db.Column(db.Float)
    pml_25 = db.Column(db.Float)
    pml_50 = db.Column(db.Float)
    pml_100 = db.Column(db.Float)
    pml_250 = db.Column(db.Float)

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


class AALFloodSawah(db.Model):
    """Flood (Banjir) rice field AAL and PML values from CSV, per kota/year/CC scenario."""
    __tablename__ = 'aal_flood_sawah'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    year = db.Column(db.Integer, nullable=False)               # 2022, 2025, 2028
    climate_change = db.Column(db.String(10), nullable=False)  # 'ncc' or 'cc'
    kota = db.Column(db.String(100), nullable=False)
    aal   = db.Column(db.Float)
    pml_10  = db.Column(db.Float)
    tvar_10 = db.Column(db.Float)
    pml_25  = db.Column(db.Float)
    tvar_25 = db.Column(db.Float)
    pml_50  = db.Column(db.Float)
    tvar_50 = db.Column(db.Float)
    pml_100 = db.Column(db.Float)
    tvar_100 = db.Column(db.Float)
    pml_250 = db.Column(db.Float)
    tvar_250 = db.Column(db.Float)

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


class AALFloodSawahSkema2(db.Model):
    """Flood (Banjir) rice field AAL and PML values for Skema 2 (7 Return Periods)."""
    __tablename__ = 'aal_flood_sawah_skema2'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    year = db.Column(db.Integer, nullable=False)               # 2022, 2025, 2028
    climate_change = db.Column(db.String(10), nullable=False)  # 'ncc' or 'cc'
    kota = db.Column(db.String(100), nullable=False)
    aal   = db.Column(db.Float)
    pml_2   = db.Column(db.Float, default=0)
    tvar_2  = db.Column(db.Float, default=0)
    pml_5   = db.Column(db.Float, default=0)
    tvar_5  = db.Column(db.Float, default=0)
    pml_10  = db.Column(db.Float, default=0)
    tvar_10 = db.Column(db.Float, default=0)
    pml_25  = db.Column(db.Float, default=0)
    tvar_25 = db.Column(db.Float, default=0)
    pml_50  = db.Column(db.Float, default=0)
    tvar_50 = db.Column(db.Float, default=0)
    pml_100 = db.Column(db.Float, default=0)
    tvar_100 = db.Column(db.Float, default=0)
    pml_250 = db.Column(db.Float, default=0)
    tvar_250 = db.Column(db.Float, default=0)

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}
<<<<<<< HEAD
=======


class AALFloodBuilding(db.Model):
    """Flood (Banjir) building AAL and PML values from CSV, per exposure/kota/CC scenario (Skema 1)."""
    __tablename__ = 'aal_flood_building'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    exposure = db.Column(db.String(100), nullable=False)
    climate_change = db.Column(db.String(100), nullable=False)
    id_kota = db.Column(db.String(100), nullable=False)
    cv = db.Column(db.Float)
    aal = db.Column(db.Float)
    var_95 = db.Column(db.Float)
    tvar_95 = db.Column(db.Float)
    var_99 = db.Column(db.Float)
    tvar_99 = db.Column(db.Float)
    pml_25 = db.Column(db.Float)
    pml_50 = db.Column(db.Float)
    pml_100 = db.Column(db.Float)
    pml_250 = db.Column(db.Float)

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}

class AALFloodBuildingSkema2(db.Model):
    """Flood (Banjir) building AAL and PML values for Skema 2 (7 Return Periods)."""
    __tablename__ = 'aal_flood_building_skema2'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    exposure = db.Column(db.String(100), nullable=False)
    climate_change = db.Column(db.String(100), nullable=False)
    kota = db.Column(db.String(100), nullable=False)
    aal = db.Column(db.Float, default=0)
    pml_2 = db.Column(db.Float, default=0)
    pml_5 = db.Column(db.Float, default=0)
    pml_10 = db.Column(db.Float, default=0)
    pml_25 = db.Column(db.Float, default=0)
    pml_50 = db.Column(db.Float, default=0)
    pml_100 = db.Column(db.Float, default=0)
    pml_250 = db.Column(db.Float, default=0)

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}

class TsunamiRiskResults(db.Model):
    """Tsunami risk metrics from CSV (VaR, TVaR)."""
    __tablename__ = 'tsunami_risk_results'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    kota = db.Column(db.String(100), nullable=False)
    exposure = db.Column(db.String(100), nullable=False)
    aal = db.Column(db.Float, default=0)
    actual_cv = db.Column(db.Float)
    var_90 = db.Column(db.Float)
    tvar_90 = db.Column(db.Float)
    var_95 = db.Column(db.Float)
    tvar_95 = db.Column(db.Float)
    var_98 = db.Column(db.Float)
    tvar_98 = db.Column(db.Float)
    var_99 = db.Column(db.Float)
    tvar_99 = db.Column(db.Float)
    var_995 = db.Column(db.Float)
    tvar_995 = db.Column(db.Float)

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}
>>>>>>> 30ae406 (Fix ActivityLog 500 error: added missing to_dict method)
