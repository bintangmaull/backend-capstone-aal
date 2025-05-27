from flask import Blueprint
from app.controller.controller_crud_hsbgn import HSBGNController

# Buat Blueprint untuk API HSBGN
hsbgn_bp = Blueprint("hsbgn_bp", __name__, url_prefix="/api/hsbgn")

# Endpoint CRUD HSBGN
hsbgn_bp.add_url_rule(
    "",
    view_func=HSBGNController.get_all,
    methods=["GET"]
)
hsbgn_bp.add_url_rule(
    "/<string:hsbgn_id>",
    view_func=HSBGNController.get_by_id,
    methods=["GET"]
)
hsbgn_bp.add_url_rule(
    "/kota/<string:kota>",
    view_func=HSBGNController.get_by_kota,
    methods=["GET"]
)
hsbgn_bp.add_url_rule(
    "",
    view_func=HSBGNController.create,
    methods=["POST"]
)
hsbgn_bp.add_url_rule(
    "/<string:hsbgn_id>",
    view_func=HSBGNController.update,
    methods=["PUT"]
)
hsbgn_bp.add_url_rule(
    "/<string:hsbgn_id>",
    view_func=HSBGNController.delete,
    methods=["DELETE"]
)

# ——— Tambahan untuk dropdown Provinsi & Kota ———

# 1) Daftar semua provinsi unik
hsbgn_bp.add_url_rule(
    "/provinsi",
    view_func=HSBGNController.get_provinsi,
    methods=["GET"]
)

# 2) Daftar kota berdasarkan provinsi terpilih
hsbgn_bp.add_url_rule(
    "/provinsi/<string:provinsi>/kota",
    view_func=HSBGNController.get_kota_by_provinsi,
    methods=["GET"]
)
