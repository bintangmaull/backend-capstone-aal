from flask import Blueprint, jsonify, request
from app.controller.controller_buffer_hazard import BufferDisasterController

bp = Blueprint(
    "buffer_disaster", __name__, url_prefix="/api/buffer"
)


def _parse_bbox(args: dict) -> dict:
    try:
        return {
            "minlng": float(args["minlng"]),
            "minlat": float(args["minlat"]),
            "maxlng": float(args["maxlng"]),
            "maxlat": float(args["maxlat"])
        }
    except (KeyError, ValueError):
        raise ValueError(
            "Parameter bbox (minlng,minlat,maxlng,maxlat) wajib numeric"
        )


@bp.route("/<dtype>", methods=["GET"])
def get_buffer(dtype: str):
    # Extract and validate query parameters
    try:
        bbox = _parse_bbox(request.args)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    tol_arg = request.args.get("tol", None)
    try:
        tol = float(tol_arg) if tol_arg is not None else 0.001
    except ValueError:
        tol = 0.001

    field = request.args.get("field")
    if not field:
        return jsonify({"error": "Parameter field wajib"}), 400

    # Delegate to controller
    fc = BufferDisasterController.get_buffer(
        dtype=dtype,
        bbox=bbox,
        field=field,
        tol=tol
    )
    return jsonify(fc)


@bp.route("/<dtype>/nearest", methods=["GET"])
def get_nearest(dtype: str):
    field = request.args.get("field")
    if not field:
        return jsonify({"error": "Parameter field wajib"}), 400

    try:
        lat = float(request.args["lat"])
        lng = float(request.args["lng"])
    except (KeyError, ValueError):
        return jsonify({"error": "lat & lng wajib numeric"}), 400

    # Delegate to controller
    data = BufferDisasterController.get_nearest(
        dtype=dtype,
        field=field,
        lat=lat,
        lng=lng
    )
    if not data:
        return jsonify({"error": "tidak ditemukan"}), 404

    return jsonify(data)
