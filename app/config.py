import os

class Config:
    """Konfigurasi utama Flask untuk aplikasi"""

    # Ambil konfigurasi database dari environment variables atau gunakan default
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD','postgres')  # Tidak ada default untuk keamanan
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'capstone')

    # Pastikan password ada agar tidak ada string kosong dalam URI
    if DB_PASSWORD:
        SQLALCHEMY_DATABASE_URI = (
            f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        )
    else:
        SQLALCHEMY_DATABASE_URI = (
            f'postgresql://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        )

    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Upload folder dengan path absolut untuk menghindari error
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    UPLOAD_FOLDER = os.getenv('UPLOAD_FOLDER', os.path.join(BASE_DIR, 'uploads'))

    # Pastikan folder upload ada
    if not os.path.exists(UPLOAD_FOLDER):
        os.makedirs(UPLOAD_FOLDER)

    # Opsi untuk debug mode
    DEBUG = os.getenv('DEBUG', 'False').lower() in ['true', '1', 't']
