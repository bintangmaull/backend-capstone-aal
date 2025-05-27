from app.service.service_buffer_hazard import BufferDisasterService


class BufferDisasterController:
    @staticmethod
    def get_buffer(dtype: str,
                   bbox: dict,
                   field: str,
                   tol: float) -> dict:
        """
        Delegates to the BufferDisasterService to compute buffer
        and returns a GeoJSON-like feature collection.
        """
        return BufferDisasterService.get_feature_collection(
            dtype=dtype,
            field=field,
            bbox=bbox,
            tol=tol
        )

    @staticmethod
    def get_nearest(dtype: str,
                    field: str,
                    lat: float,
                    lng: float) -> dict:
        """
        Delegates to the BufferDisasterService to find the nearest feature
        and returns its properties as a dict (or None if not found).
        """
        return BufferDisasterService.get_nearest(
            dtype=dtype,
            field=field,
            lat=lat,
            lng=lng
        )