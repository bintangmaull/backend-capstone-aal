import os
import tempfile
import requests
from requests.auth import HTTPBasicAuth

import rasterio
import numpy as np
from mapclassify import NaturalBreaks

from app.service.service_visualisasi_hazard import RasterService

GEOSERVER_URL  = "http://localhost:8081/geoserver"
GEOSERVER_USER = "admin"
GEOSERVER_PASS = "geoserver"
WORKSPACE      = "ne"

# definisi bencana → kolom
BENCANA_MAP = {
    'gempa':       ['mmi_100',    'mmi_250',   'mmi_500'],
    'banjir':      ['depth_25',   'depth_50',  'depth_100'],
    'longsor':     ['mflux_2',    'mflux_5'],
    'gunungberapi':['kpa_50',     'kpa_100',   'kpa_250']
}


def compute_breaks(tif_path, k=5):
    """Hitung natural breaks (Jenks) hanya dari data > 0."""
    with rasterio.open(tif_path) as src:
        arr = src.read(1)
        mask = (arr != src.nodata) & (arr > 0)
        data = arr[mask].astype(float)
    if data.size == 0:
        return []
    nb = NaturalBreaks(data, k=k)
    return nb.bins.tolist()


def make_sld(layer_name, breaks):
    """
    Bangun SLD:
     - quantity=0 → hijau sangat tua
     - kemudian kelas‐kelas Jenks untuk nilai > 0
    """
    # Warna: pertama untuk 0, lalu untuk setiap kelas Jenks
    zero_color = "#004d00"  # hijau sangat tua
    pos_colors = ["#006400", "#66cc00", "#edd16d", "#cc6600", "#ff0000"]

    entries = []
    # 1) Entry untuk nol
    entries.append(f"""
      <ColorMapEntry
        color="{zero_color}"
        quantity="0.0"
        label="0"/>
    """)

    # 2) Entry untuk natural breaks > 0
    lower = 0.0
    for i, thr in enumerate(breaks):
        upper = thr
        color = pos_colors[i] if i < len(pos_colors) else pos_colors[-1]
        entries.append(f"""
      <ColorMapEntry
        color="{color}"
        quantity="{upper:.6f}"
        label="{lower:.2f}-{upper:.2f}"/>
        """)
        lower = upper

    cmap = "\n".join(entries)

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor version="1.0.0"
    xmlns="http://www.opengis.net/sld"
    xmlns:ogc="http://www.opengis.net/ogc"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="
      http://www.opengis.net/sld 
      http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd
    ">
   <NamedLayer>
     <Name>{layer_name}</Name>
     <UserStyle>
       <Title>{layer_name} Natural Breaks</Title>
       <FeatureTypeStyle>
         <Rule>
          <RasterSymbolizer>
            <VendorOption name="interpolation">bilinear</VendorOption>
           <ColorMap type="interval">
             {cmap}
           </ColorMap>
         </RasterSymbolizer>
       </Rule>
     </FeatureTypeStyle>
   </UserStyle>
 </NamedLayer>
</StyledLayerDescriptor>"""


def upload_style(layer_name, tif_path):
    """Upload SLD ke GeoServer, lalu assign ke layer."""
    # 1) hitung kelas
    breaks = compute_breaks(tif_path, k=5)

    # 2) buat SLD
    sld_body   = make_sld(layer_name, breaks)
    style_name = f"{layer_name}_jb"

    # 3) POST untuk create style, jika sudah ada -> PUT
    url_styles  = f"{GEOSERVER_URL}/rest/styles"
    url_style   = f"{GEOSERVER_URL}/rest/styles/{style_name}"
    headers_sld = {"Content-Type": "application/vnd.ogc.sld+xml"}
    params_post = {"name": style_name}

    r1 = requests.post(
        url_styles,
        params=params_post,
        data=sld_body.encode('utf-8'),
        auth=HTTPBasicAuth(GEOSERVER_USER, GEOSERVER_PASS),
        headers=headers_sld
    )
    if r1.status_code == 403 and "already exists" in r1.text:
        r1 = requests.put(
            url_style,
            data=sld_body.encode('utf-8'),
            auth=HTTPBasicAuth(GEOSERVER_USER, GEOSERVER_PASS),
            headers=headers_sld
        )
    if r1.status_code not in (200, 201):
        raise RuntimeError(f"Failed to create/update style {style_name}: {r1.status_code}\n{r1.text}")

    # 4) Assign style ini sebagai default ke coverage layer
    url_layer = f"{GEOSERVER_URL}/rest/layers/{WORKSPACE}:{layer_name}"
    xml = f"""
    <layer>
      <defaultStyle>
        <name>{style_name}</name>
      </defaultStyle>
    </layer>
    """
    r2 = requests.put(
        url_layer,
        data=xml,
        auth=HTTPBasicAuth(GEOSERVER_USER, GEOSERVER_PASS),
        headers={"Content-Type": "application/xml"}
    )
    if r2.status_code not in (200, 201):
        raise RuntimeError(f"Failed to assign style {style_name}: {r2.status_code}\n{r2.text}")

    return r1.status_code, r2.status_code


def upload_all_geotiffs():
    """
    1) Generate GeoTIFF via RasterService
    2) Upload ke GeoServer
    3) Hitung & upload SLD, assign ke layer
    """
    hasil = []

    for bencana, koloms in BENCANA_MAP.items():
        for kolom in koloms:
            layer_name = f"hazard_{bencana}_{kolom}"
            rec = {'layer': layer_name}
            try:
                # generate .tif
                tif_path, err = RasterService.generate_raster_from_points(bencana, kolom)
                if err:
                    rec.update(status='error', message=err)
                    hasil.append(rec)
                    continue

                # upload GeoTIFF
                url = (
                    f"{GEOSERVER_URL}/rest/workspaces/{WORKSPACE}"
                    f"/coveragestores/{layer_name}/file.geotiff"
                    f"?configure=all&coverageName={layer_name}"
                )
                with open(tif_path, 'rb') as f:
                    r = requests.put(
                        url, data=f,
                        auth=HTTPBasicAuth(GEOSERVER_USER,GEOSERVER_PASS),
                        headers={"Content-Type":"image/tiff"}
                    )
                rec['upload_code'] = r.status_code
                if r.status_code not in (200,201):
                    rec.update(status='error', message=r.text.strip())
                    hasil.append(rec)
                    continue

                # upload & assign style
                sc1, sc2 = upload_style(layer_name, tif_path)
                rec.update(status='success', style_codes=(sc1, sc2))

            except Exception as e:
                rec.update(status='error', message=str(e))

            hasil.append(rec)

    return hasil
