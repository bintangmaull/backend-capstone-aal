from app.extensions import db
from geoalchemy2 import Geometry

# === Raw Data Tables (Input) ===
class RawGempa(db.Model):
    __tablename__ = 'model_intensitas_gempa'
    id_lokasi = db.Column(db.Integer, primary_key=True)
    lon = db.Column(db.Float)
    lat = db.Column(db.Float)
    mmi_500 = db.Column(db.Float)
    mmi_250 = db.Column(db.Float)
    mmi_100 = db.Column(db.Float)
    geom = db.Column(Geometry('POINT', srid=4326))

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


class RawLongsor(db.Model):
    __tablename__ = 'model_intensitas_longsor'
    id_lokasi = db.Column(db.Float, primary_key=True)
    lon = db.Column(db.Float)
    lat = db.Column(db.Float)
    mflux_5 = db.Column(db.Float)
    mflux_2 = db.Column(db.Float)
    geom = db.Column(Geometry('POINT', srid=4326))

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


class RawGunungBerapi(db.Model):
    __tablename__ = 'model_intensitas_gunungberapi'
    id_lokasi = db.Column(db.Float, primary_key=True)
    lon = db.Column(db.Float)
    lat = db.Column(db.Float)
    kpa_250 = db.Column(db.Float)
    kpa_100 = db.Column(db.Float)
    kpa_50 = db.Column(db.Float)
    geom = db.Column(Geometry('POINT', srid=4326))

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


class RawBanjir(db.Model):
    __tablename__ = 'model_intensitas_banjir'
    id_lokasi = db.Column(db.Float, primary_key=True)
    lon = db.Column(db.Float)
    lat = db.Column(db.Float)
    depth_100 = db.Column(db.Float)
    depth_50 = db.Column(db.Float)
    depth_25 = db.Column(db.Float)
    geom = db.Column(Geometry('POINT', srid=4326))

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


# === Processed Data Tables (Output) ===
class HasilProsesGempa(db.Model):
    __tablename__ = 'dmgratio_gempa'

    id_lokasi = db.Column(db.Integer, primary_key=True, autoincrement=True)

    dmgratio_cr_mmi500 = db.Column(db.Float, nullable=True)
    dmgratio_mcf_mmi500 = db.Column(db.Float, nullable=True)
    dmgratio_mur_mmi500 = db.Column(db.Float, nullable=True)
    dmgratio_lightwood_mmi500 = db.Column(db.Float, nullable=True)

    dmgratio_cr_mmi250 = db.Column(db.Float, nullable=True)
    dmgratio_mcf_mmi250 = db.Column(db.Float, nullable=True)
    dmgratio_mur_mmi250 = db.Column(db.Float, nullable=True)
    dmgratio_lightwood_mmi250 = db.Column(db.Float, nullable=True)

    dmgratio_cr_mmi100 = db.Column(db.Float, nullable=True)
    dmgratio_mcf_mmi100 = db.Column(db.Float, nullable=True)
    dmgratio_mur_mmi100 = db.Column(db.Float, nullable=True)
    dmgratio_lightwood_mmi100 = db.Column(db.Float, nullable=True)

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


class HasilProsesBanjir(db.Model):
    __tablename__ = 'dmgratio_banjir_copy'

    id_lokasi = db.Column(db.Integer, primary_key=True, autoincrement=True)

    dmgratio_1_depth100 = db.Column(db.Float, nullable=True, default=0)
    dmgratio_2_depth100 = db.Column(db.Float, nullable=True, default=0)


    dmgratio_1_depth50 = db.Column(db.Float, nullable=True, default=0)
    dmgratio_2_depth50 = db.Column(db.Float, nullable=True, default=0)


    dmgratio_1_depth25 = db.Column(db.Float, nullable=True, default=0)
    dmgratio_2_depth25 = db.Column(db.Float, nullable=True, default=0)


    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


class HasilProsesLongsor(db.Model):
    __tablename__ = 'dmgratio_longsor'

    id_lokasi = db.Column(db.Integer, primary_key=True, autoincrement=True)

    dmgratio_cr_mflux5 = db.Column(db.Float, nullable=True)
    dmgratio_mcf_mflux5 = db.Column(db.Float, nullable=True)
    dmgratio_mur_mflux5 = db.Column(db.Float, nullable=True)
    dmgratio_lightwood_mflux5 = db.Column(db.Float, nullable=True)

    dmgratio_cr_mflux2 = db.Column(db.Float, nullable=True)
    dmgratio_mcf_mflux2 = db.Column(db.Float, nullable=True)
    dmgratio_mur_mflux2 = db.Column(db.Float, nullable=True)
    dmgratio_lightwood_mflux2 = db.Column(db.Float, nullable=True)


    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


class HasilProsesGunungBerapi(db.Model):
    __tablename__ = 'dmgratio_gunungberapi'

    id_lokasi = db.Column(db.Integer, primary_key=True, autoincrement=True)

    dmgratio_cr_kpa250 = db.Column(db.Float, nullable=True)
    dmgratio_mcf_kpa250 = db.Column(db.Float, nullable=True)
    dmgratio_mur_kpa250 = db.Column(db.Float, nullable=True)
    dmgratio_lightwood_kpa250 = db.Column(db.Float, nullable=True)

    dmgratio_cr_kpa100 = db.Column(db.Float, nullable=True)
    dmgratio_mcf_kpa100 = db.Column(db.Float, nullable=True)
    dmgratio_mur_kpa100 = db.Column(db.Float, nullable=True)
    dmgratio_lightwood_kpa100 = db.Column(db.Float, nullable=True)

    dmgratio_cr_kpa50 = db.Column(db.Float, nullable=True)
    dmgratio_mcf_kpa50 = db.Column(db.Float, nullable=True)
    dmgratio_mur_kpa50 = db.Column(db.Float, nullable=True)
    dmgratio_lightwood_kpa50 = db.Column(db.Float, nullable=True)

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}



# === Data HSBGN ===
class HSBGN(db.Model):
    __tablename__ = 'kota'

    id_kota = db.Column(db.Integer, primary_key=True, autoincrement=True)
    kota = db.Column(db.String(255), nullable=False, unique=True)
    provinsi    = db.Column(db.String(255), nullable=False, unique=True)
    
    hsbgn = db.Column(db.Float, nullable=False)

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}

class Bangunan(db.Model):
    __tablename__ = "bangunan_copy"
    
    id_bangunan = db.Column(db.String(50), primary_key=True)
    lon = db.Column(db.Float, nullable=False)
    lat = db.Column(db.Float, nullable=False)
    taxonomy = db.Column(db.String(100), nullable=False)
    luas = db.Column(db.Float, nullable=False)
    nama_gedung = db.Column(db.String(255))
    alamat = db.Column(db.String(255))
    kota = db.Column(db.String(255), nullable=False, unique=True)
    provinsi = db.Column(db.String(255), nullable=False, unique=True)
    geom = db.Column(Geometry('POINT', srid=4326), nullable=False)
    jumlah_lantai = db.Column(db.Integer)
    kode_bangunan = db.Column(db.String(255))
    

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}
    
# === Provinsi (New Table) ===
class Provinsi(db.Model):
    __tablename__ = 'provinsi'
    id_provinsi = db.Column(db.Integer, primary_key=True)
    provinsi    = db.Column(db.String(255), nullable=False, unique=True)
    geom        = db.Column(Geometry('MULTIPOLYGON', srid=4326))

    def to_dict(self):
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}


class HasilProsesDirectLoss(db.Model):
    __tablename__ = 'hasil_proses_directloss'

    id_bangunan = db.Column(db.String, primary_key=True, nullable=False)
    
    direct_loss_gempa_500 = db.Column(db.Float, default=0)
    direct_loss_gempa_250 = db.Column(db.Float, default=0)
    direct_loss_gempa_100 = db.Column(db.Float, default=0)
    
    direct_loss_banjir_100 = db.Column(db.Float, default=0)
    direct_loss_banjir_50 = db.Column(db.Float, default=0)
    direct_loss_banjir_25 = db.Column(db.Float, default=0)

    direct_loss_gunungberapi_250 = db.Column(db.Float, default=0)
    direct_loss_gunungberapi_100 = db.Column(db.Float, default=0)
    direct_loss_gunungberapi_50 = db.Column(db.Float, default=0)

    direct_loss_longsor_5 = db.Column(db.Float, default=0)
    direct_loss_longsor_2 = db.Column(db.Float, default=0)

    def to_dict(self):
        """Mengembalikan representasi dictionary dari objek hasil proses direct loss."""
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}
    
class HasilAALProvinsi(db.Model):
    __tablename__ = 'hasil_aal_provinsi'
    provinsi    = db.Column(db.String(255), primary_key=True, nullable=False, unique=True)

    # Gempa
    aal_gempa_500_bmn = db.Column(db.Float, default=0)
    aal_gempa_500_fs = db.Column(db.Float, default=0)
    aal_gempa_500_fd = db.Column(db.Float, default=0)
    aal_gempa_500_total = db.Column(db.Float, default=0)

    aal_gempa_250_bmn = db.Column(db.Float, default=0)
    aal_gempa_250_fs = db.Column(db.Float, default=0)
    aal_gempa_250_fd = db.Column(db.Float, default=0)
    aal_gempa_250_total = db.Column(db.Float, default=0)

    aal_gempa_100_bmn = db.Column(db.Float, default=0)
    aal_gempa_100_fs = db.Column(db.Float, default=0)
    aal_gempa_100_fd = db.Column(db.Float, default=0)
    aal_gempa_100_total = db.Column(db.Float, default=0)

    # Banjir
    aal_banjir_100_bmn = db.Column(db.Float, default=0)
    aal_banjir_100_fs = db.Column(db.Float, default=0)
    aal_banjir_100_fd = db.Column(db.Float, default=0)
    aal_banjir_100_total = db.Column(db.Float, default=0)

    aal_banjir_50_bmn = db.Column(db.Float, default=0)
    aal_banjir_50_fs = db.Column(db.Float, default=0)
    aal_banjir_50_fd = db.Column(db.Float, default=0)
    aal_banjir_50_total = db.Column(db.Float, default=0)

    aal_banjir_25_bmn = db.Column(db.Float, default=0)
    aal_banjir_25_fs = db.Column(db.Float, default=0)
    aal_banjir_25_fd = db.Column(db.Float, default=0)
    aal_banjir_25_total = db.Column(db.Float, default=0)

    # Gunung Berapi
    aal_gunungberapi_250_bmn = db.Column(db.Float, default=0)
    aal_gunungberapi_250_fs = db.Column(db.Float, default=0)
    aal_gunungberapi_250_fd = db.Column(db.Float, default=0)
    aal_gunungberapi_250_total = db.Column(db.Float, default=0)

    aal_gunungberapi_100_bmn = db.Column(db.Float, default=0)
    aal_gunungberapi_100_fs = db.Column(db.Float, default=0)
    aal_gunungberapi_100_fd = db.Column(db.Float, default=0)
    aal_gunungberapi_100_total = db.Column(db.Float, default=0)

    aal_gunungberapi_50_bmn = db.Column(db.Float, default=0)
    aal_gunungberapi_50_fs = db.Column(db.Float, default=0)
    aal_gunungberapi_50_fd = db.Column(db.Float, default=0)
    aal_gunungberapi_50_total = db.Column(db.Float, default=0)

    #longsor

    aal_longsor_5_bmn = db.Column(db.Float, default=0)
    aal_longsor_5_fs = db.Column(db.Float, default=0)
    aal_longsor_5_fd = db.Column(db.Float, default=0)
    aal_longsor_5_total = db.Column(db.Float, default=0)

    aal_longsor_2_bmn = db.Column(db.Float, default=0)
    aal_longsor_2_fs = db.Column(db.Float, default=0)
    aal_longsor_2_fd = db.Column(db.Float, default=0)
    aal_longsor_2_total = db.Column(db.Float, default=0)


    def to_dict(self):
        """Mengubah objek menjadi dictionary agar mudah digunakan."""
        return {column.name: getattr(self, column.name) for column in self.__table__.columns}


# Kurva Kerentanan

class GempaReferenceCurve(db.Model):
    __tablename__ = "referensi_dmgratio_gempa"

    id_referensi = db.Column(db.Integer, primary_key=True)
    tipe_kurva = db.Column(db.String(10), nullable=False)
    x = db.Column(db.Float, nullable=False)
    y = db.Column(db.Float, nullable=False)

class GunungBerapiReferenceCurve(db.Model):
    __tablename__ = "referensi_dmgratio_gunungberapi"

    id_referensi = db.Column(db.Integer, primary_key=True)
    tipe_kurva = db.Column(db.String(10), nullable=False)
    x = db.Column(db.Float, nullable=False)
    y = db.Column(db.Float, nullable=False)

class LongsorReferenceCurve(db.Model):
    __tablename__ = "referensi_dmgratio_longsor"

    id_referensi = db.Column(db.Integer, primary_key=True)
    tipe_kurva = db.Column(db.String(10), nullable=False)
    x = db.Column(db.Float, nullable=False)
    y = db.Column(db.Float, nullable=False)

class BanjirReferenceCurve(db.Model):
    __tablename__ = "referensi_dmgratio_banjir"

    id_referensi = db.Column(db.Integer, primary_key=True)
    tipe_kurva = db.Column(db.String(10), nullable=False)
    x = db.Column(db.Float, nullable=False)
    y = db.Column(db.Float, nullable=False)
