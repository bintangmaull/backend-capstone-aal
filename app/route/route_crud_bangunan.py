from flask import Blueprint
from app.controller.controller_crud_bangunan import BangunanController

bangunan_bp = Blueprint("bangunan_bp", __name__, url_prefix="/api")

# CRUD endpoints
bangunan_bp.add_url_rule(
    "/bangunan", view_func=BangunanController.get_all, methods=["GET"]
)
bangunan_bp.add_url_rule(
    "/bangunan/<string:bangunan_id>",
    view_func=BangunanController.get_by_id,
    methods=["GET"]
)
bangunan_bp.add_url_rule(
    "/bangunan", view_func=BangunanController.create, methods=["POST"]
)
bangunan_bp.add_url_rule(
    "/bangunan/<string:bangunan_id>",
    view_func=BangunanController.update,
    methods=["PUT"]
)
bangunan_bp.add_url_rule(
    "/bangunan/<string:bangunan_id>/<string:prov>",
    view_func=BangunanController.delete,
    methods=["DELETE"]
)

# Helper untuk generate ID, dan daftar provinsi/kota
bangunan_bp.add_url_rule(
    "/bangunan/new-id", view_func=BangunanController.new_id, methods=["GET"]
)
bangunan_bp.add_url_rule(
    "/bangunan/provinsi", view_func=BangunanController.get_provinsi_list, methods=["GET"]
)
bangunan_bp.add_url_rule(
    "/bangunan/kota", view_func=BangunanController.get_kota_list, methods=["GET"]
)

# Endpoint untuk upload CSV
bangunan_bp.add_url_rule(
    "/bangunan/upload",
    view_func=BangunanController.upload_csv,
    methods=["POST"]
)

# Recalc directloss & AAL untuk satu bangunan
bangunan_bp.add_url_rule(
    "/bangunan/<string:bangunan_id>/recalc",
    view_func=BangunanController.recalc,
    methods=["POST"]
)
