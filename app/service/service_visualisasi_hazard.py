import os
import tempfile
import numpy as np
import geopandas as gpd
import rasterio
import logging
from rasterio.transform import from_origin
from rasterio.mask import mask
from rasterio.features import rasterize
from scipy.spatial import cKDTree
from scipy.interpolate import griddata
from app.repository.repo_visualisasi_hazard import IntensitasRepo
from app import db

logger = logging.getLogger(__name__)

class RasterService:
    @staticmethod
    def generate_raster_from_points(bencana, kolom):
        logger.info(f"📥 Mulai generate raster untuk {bencana} - {kolom}")
        points = IntensitasRepo.get_points_by_bencana(bencana, kolom)
        if not points:
            logger.warning("⚠️ Tidak ada data titik ditemukan.")
            return None, "No data found"

        xs = np.array([p['x'] for p in points])
        ys = np.array([p['y'] for p in points])
        zs = np.array([p.get(kolom) if p.get(kolom) is not None else 0 for p in points])
        logger.info(f"✅ Jumlah titik: {len(points)}")

        pixel_size = 0.01

        # Ambil bounds seluruh Indonesia dari tabel provinsi
        clip_gdf = gpd.read_postgis(
            """
            SELECT ST_CollectionExtract(ST_UnaryUnion(ST_MakeValid(geom)), 3) AS geom
            FROM provinsi
            """,
            db.engine,
            geom_col='geom'
        ).to_crs(epsg=4326)
        minx, miny, maxx, maxy = clip_gdf.total_bounds
        logger.info(f"📦 Bounds Indonesia: {minx},{miny} – {maxx},{maxy}")

        # Hitung ukuran grid berdasarkan bounds provinsi, bukan titik saja
        width  = int(np.ceil((maxx - minx) / pixel_size))
        height = int(np.ceil((maxy - miny) / pixel_size))
        logger.info(f"🧱 Ukuran raster: {width} x {height}")

        # Buat meshgrid dari bounds provinsi
        xi = np.linspace(minx, maxx, width)
        yi = np.linspace(maxy, miny, height)  # flipped so origin top-left
        grid_x, grid_y = np.meshgrid(xi, yi)

        logger.info("⚙️ Mulai interpolasi IDW...")
        grid_z = RasterService.idw_interpolation(xs, ys, zs, grid_x, grid_y)


        logger.info("🔄 Mengisi NaN dengan nearest-neighbor…")
        nearest = griddata(
            (xs, ys),
            zs,
            (grid_x, grid_y),
            method='nearest'
        )
        # hanya timpa yg NaN
        grid_z = np.where(np.isnan(grid_z), nearest, grid_z)
        grid_z = np.nan_to_num(grid_z, nan=0.0)

        # Buat transform full‐Indonesia
        transform = from_origin(minx, maxy, pixel_size, pixel_size)

        logger.info("🧩 Membuat mask daratan untuk seluruh grid...")
        shapes = ((geom, 1) for geom in clip_gdf.geometry)
        mask_arr = rasterize(
            shapes=shapes,
            out_shape=(height, width),
            transform=transform,
            fill=0,
            default_value=1,
            dtype='uint8'
        )

        # Siapkan array akhir: isi grid_z di darat, nodata di laut
        nodata_value = -9999.0
        arr = np.full((height, width), nodata_value, dtype='float32')
        # grid_z mungkin mengandung nan, ganti ke 0 sebelum masking
        grid_z = np.nan_to_num(grid_z, nan=0.0)
        # isi nilai interpolasi hanya di daratan (mask_arr==1)
        arr[mask_arr == 1] = grid_z[mask_arr == 1]

        temp_dir = tempfile.gettempdir()
        raw_raster_path = os.path.join(temp_dir, f'{bencana}_{kolom}_raw.tif')

        logger.info(f"💾 Menyimpan raster mentah ke {raw_raster_path}")
        with rasterio.open(
            raw_raster_path,
            'w',
            driver='GTiff',
            height=height,
            width=width,
            count=1,
            dtype='float32',
            crs='+proj=longlat +datum=WGS84 +no_defs',
            transform=transform,
            nodata=nodata_value
        ) as dst:
            dst.write(arr, 1)

        # Hapus crop=True agar ukuran raster tetap full‐Indonesia
        logger.info("✂️ Memotong raster sesuai geometri batas (tanpa crop)...")
        with rasterio.open(raw_raster_path) as src:
            out_image, out_transform = mask(
                src,
                clip_gdf.geometry,
                nodata=nodata_value  # crop=False by default
            )
            out_meta = src.meta.copy()

        out_meta.update({
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform,
            "nodata": nodata_value
        })

        final_raster_path = os.path.join(temp_dir, f'{bencana}_{kolom}_clipped.tif')
        logger.info(f"💾 Menyimpan raster akhir ke {final_raster_path}")
        with rasterio.open(final_raster_path, "w", **out_meta) as dest:
            dest.write(out_image)

        logger.info("🗂️ Simpan raster ke PostGIS...")
        RasterService.save_to_postgis(final_raster_path, bencana, kolom)
        logger.info("✅ Proses selesai")

        return final_raster_path, None

    @staticmethod
    def idw_interpolation(x, y, z, xi, yi, power=2):
        xyz = np.vstack((x, y)).T
        tree = cKDTree(xyz)
        flat_grid = np.vstack((xi.flatten(), yi.flatten())).T

        dist, idx = tree.query(flat_grid, k=6)
        weights = 1 / (dist**power + 1e-8)
        z_values = np.take(z, idx)
        weighted = np.sum(weights * z_values, axis=1) / np.sum(weights, axis=1)

        return weighted.reshape(xi.shape)

    @staticmethod
    def save_to_postgis(tif_path, bencana, kolom):
        from psycopg2 import connect
        logger.info(f"📤 Membaca {tif_path} untuk disimpan ke PostGIS...")
        conn = db.engine.raw_connection()
        cur = conn.cursor()
        with open(tif_path, 'rb') as f:
            rast_data = f.read()
        query = """
        INSERT INTO hazard_raster (bencana, kolom, rast)
        VALUES (%s, %s, ST_FromGDALRaster(%s));
        """
        try:
            cur.execute(query, (bencana, kolom, rast_data))
            conn.commit()
            logger.info("✅ Raster berhasil disimpan ke tabel hazard_raster")
        except Exception as e:
            logger.error(f"❌ Gagal menyimpan raster ke PostGIS: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()
