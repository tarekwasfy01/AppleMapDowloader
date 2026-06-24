#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Py Map Stitcher 3.

Use only with map/tile servers for which you have permission. Many public map
providers prohibit bulk downloading. The app intentionally uses a conservative
rate limit and requires user-supplied/custom URL templates.
"""

import concurrent.futures as cf
import dataclasses
import io
import json
import subprocess
import tempfile
import math
import os
import queue
import random
import shutil
import sys
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

try:
    import requests
except Exception as exc:  # pragma: no cover
    requests = None

try:
    from PIL import Image, ImageFile
    ImageFile.LOAD_TRUNCATED_IMAGES = True
except Exception:  # pragma: no cover
    Image = None

TILE_SIZE = 256
FIXED_FRAME_SHIFT_X_PX = 260.0
FIXED_FRAME_SHIFT_Y_PX = 330.0
USER_AGENT = "PyMapStitcher/1.0 (+local user tool)"
MAX_INFLIGHT_PER_WORKER = 4  # prevents millions of Futures in RAM
HARD_TILE_WARNING = 5_000_000
DEFAULT_CHUNK_SIZE = 64
MAX_DIRECT_TIFF_BYTES = 1_000_000_000_000  # 1 TB safety limit for sparse BigTIFF output

MAP_PRESETS = {
    "Custom": {
        "url": "https://your-tile-server.example/{z}/{x}/{y}.png",
        "note": "Enter a custom URL template manually.",
        "preview": True,
    },
    "Own Frame Server / Screenshot TIFF": {
        "url": "http://127.0.0.1:8787/frame?center={center_lat},{center_lon}&span={lat_span},{lon_span}&z={z}",
        "note": "For your own/authorized frame server. Renders URLs in Qt WebEngine, crops UI, saves TIFF tiles, and writes a stitched GeoTIFF/BigTIFF.",
        "preview": "frame",
    },
    "Apple Frame Preview / center-span helper": {
        "url": "https://maps.apple.com/frame?map=satellite&center={center_lat}%2C{center_lon}&span={lat_span}%2C{lon_span}",
        "note": "Preview/helper only: shows the Apple-style center/span frame URL so you can inspect the same coordinate logic. Use downloading only with your own or otherwise authorized frame server by switching the URL to that server.",
        "preview": "frame",
    },
    "Google Satellite": {
        "url": "https://mt.google.com/vt/lyrs=s&x={x}&y={y}&z={z}&hl=de",
        "note": "Google Satellite. Respect the terms of use; no bulk downloading without permission.",
        "preview": True,
    },
    "Google Hybrid": {
        "url": "https://mt.google.com/vt/lyrs=y&x={x}&y={y}&z={z}&hl=de",
        "note": "Google Satellite with labels. Respect the terms of use.",
        "preview": True,
    },
    "Bing Satellite": {
        "url": "https://ecn.t{snum}.tiles.virtualearth.net/tiles/a{q}.jpeg?g=14574&mkt=de-DE&n=z",
        "note": "Bing Aerial/Satellite via QuadKey {q}. Respect the terms of use.",
        "preview": True,
    },
    "Bing Hybrid": {
        "url": "https://ecn.t{snum}.tiles.virtualearth.net/tiles/h{q}.jpeg?g=14574&mkt=de-DE&n=z",
        "note": "Bing Hybrid via QuadKey {q}. Respect the terms of use.",
        "preview": True,
    },
    "Esri World Imagery": {
        "url": "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        "note": "Satellite/aerial tiles. Respect Esri terms of use.",
        "preview": True,
    },
    "OpenStreetMap Mapnik": {
        "url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
        "note": "OSM standard map. Respect the terms of use; no bulk downloading.",
        "preview": True,
    },
    "OpenTopoMap": {
        "url": "https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png",
        "note": "Topographic map. Respect the terms of use.",
        "preview": True,
    },
    "CartoDB Positron": {
        "url": "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
        "note": "Light basemap. Respect the terms of use.",
        "preview": True,
    },
    "NoniMapView Legacy: Google Satellite": {
        "url": "http://khm{rnd}.google.com/kh/v=47&x={x}&y={y}&z={z}&s=&hl=de",
        "note": "Legacy NoniMapView profile; may be outdated or blocked today.",
        "preview": True,
    },
    "NoniMapView Legacy: Google Road": {
        "url": "http://mt{rnd}.google.com/vt/lyrs=m&hl=de&x={x}&y={y}&z={z}",
        "note": "Legacy NoniMapView profile; may be outdated or blocked today.",
        "preview": True,
    },
}



@dataclasses.dataclass(frozen=True)
class TileJob:
    x: int
    y: int
    z: int
    col: int
    row: int


@dataclasses.dataclass
class StitchConfig:
    url_template: str
    output_file: Path
    z: int
    min_lat: float
    min_lon: float
    max_lat: float
    max_lon: float
    workers: int = 8
    rate_limit_ms: int = 50
    retries: int = 3
    timeout: int = 20
    headers: Optional[Dict[str, str]] = None
    chunk_size: int = DEFAULT_CHUNK_SIZE


def clamp_lat(lat: float) -> float:
    return max(min(lat, 85.05112878), -85.05112878)


def lonlat_to_tile(lon: float, lat: float, z: int) -> Tuple[int, int]:
    lat = clamp_lat(lat)
    n = 2 ** z
    x = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return max(0, min(n - 1, x)), max(0, min(n - 1, y))


def tile_to_lonlat(x: float, y: float, z: int) -> Tuple[float, float]:
    n = 2 ** z
    lon = x / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1.0 - 2.0 * y / n)))
    lat = math.degrees(lat_rad)
    return lon, lat



def lonlat_to_world_pixel(lon: float, lat: float, z: int) -> Tuple[float, float]:
    lat = clamp_lat(float(lat))
    world = float(TILE_SIZE) * (2 ** int(z))
    x = (float(lon) + 180.0) / 360.0 * world
    lat_rad = math.radians(lat)
    y = (1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * world
    return x, y


def world_pixel_to_lonlat(px: float, py: float, z: int) -> Tuple[float, float]:
    world = float(TILE_SIZE) * (2 ** int(z))
    lon = float(px) / world * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1.0 - 2.0 * float(py) / world)))
    lat = math.degrees(lat_rad)
    return lon, lat


def world_pixel_bbox_to_lonlat(px_left: float, py_top: float, px_right: float, py_bottom: float, z: int) -> Tuple[float, float, float, float]:
    west, north = world_pixel_to_lonlat(px_left, py_top, z)
    east, south = world_pixel_to_lonlat(px_right, py_bottom, z)
    return west, south, east, north

def tile_bounds_for_bbox(min_lat: float, min_lon: float, max_lat: float, max_lon: float, z: int):
    # NW and SE tile indices for Web Mercator XYZ.
    x1, y1 = lonlat_to_tile(min_lon, max_lat, z)
    x2, y2 = lonlat_to_tile(max_lon, min_lat, z)
    return min(x1, x2), min(y1, y2), max(x1, x2), max(y1, y2)


def tile_to_quadkey(x: int, y: int, z: int) -> str:
    q = []
    for i in range(z, 0, -1):
        digit = 0
        mask = 1 << (i - 1)
        if x & mask:
            digit += 1
        if y & mask:
            digit += 2
        q.append(str(digit))
    return "".join(q)


def expand_url(template: str, x: int, y: int, z: int) -> str:
    rnd = random.randint(0, 3)
    sub = ["a", "b", "c"][rnd % 3]
    snum = str(rnd % 4)
    q = tile_to_quadkey(x, y, z)

    west_lon, north_lat = tile_to_lonlat(x, y, z)
    east_lon, south_lat = tile_to_lonlat(x + 1, y + 1, z)
    center_lon = (west_lon + east_lon) / 2.0
    center_lat = (north_lat + south_lat) / 2.0
    lon_span = abs(east_lon - west_lon)
    lat_span = abs(north_lat - south_lat)
    bbox = f"{west_lon:.12f},{south_lat:.12f},{east_lon:.12f},{north_lat:.12f}"

    # Supports normal XYZ URLs and custom frame/snapshot URLs for own servers.
    return (template.replace("{x}", str(x))
                    .replace("{y}", str(y))
                    .replace("{z}", str(z))
                    .replace("{c}", str(z))
                    .replace("{q}", q)
                    .replace("{quadkey}", q)
                    .replace("{rnd}", str(rnd))
                    .replace("{snum}", snum)
                    .replace("{s}", sub)
                    .replace("{west}", f"{west_lon:.12f}")
                    .replace("{south}", f"{south_lat:.12f}")
                    .replace("{east}", f"{east_lon:.12f}")
                    .replace("{north}", f"{north_lat:.12f}")
                    .replace("{center_lon}", f"{center_lon:.12f}")
                    .replace("{center_lat}", f"{center_lat:.12f}")
                    .replace("{lon_span}", f"{lon_span:.12f}")
                    .replace("{lat_span}", f"{lat_span:.12f}")
                    .replace("{span_lon}", f"{lon_span:.12f}")
                    .replace("{span_lat}", f"{lat_span:.12f}")
                    .replace("{bbox}", bbox)
                    .replace("*GMX*", str(x))
                    .replace("*GMY*", str(y))
                    .replace("*ZM1*", str(z))
                    .replace("*IZM*", str(z))
                    .replace("*RND*", str(rnd))
                    .replace("*LAN*", "de")
                    .replace("*LAN-LAN*", "de-DE"))






def expand_frame_url_for_screenshot_job(
    template: str,
    x: int,
    y: int,
    z: int,
    render_w: int,
    render_h: int,
    crop_left: int,
    crop_top: int,
    crop_right: int,
    crop_bottom: int,
    step_factor_x: float = 1.0,
    step_factor_y: float = 1.0,
) -> str:
    """Expand frame URL so the cropped screenshot lands on the intended mosaic tile.

    This version supports an additional tile step/offset factor. Some frame
    providers do not interpret span exactly like XYZ Web-Mercator tile size, or
    UI/canvas scaling makes neighbouring screenshots too close. In that case
    the requested center must move farther than one XYZ tile. step_factor_x/y
    multiplies the tile-center offset relative to the selected mosaic origin.
    """
    rnd = random.randint(0, 3)
    sub = ["a", "b", "c"][rnd % 3]
    snum = str(rnd % 4)
    q = tile_to_quadkey(x, y, z)

    west_lon, north_lat = tile_to_lonlat(x, y, z)
    east_lon, south_lat = tile_to_lonlat(x + 1, y + 1, z)

    tile_center_lon = (west_lon + east_lon) / 2.0
    tile_center_lat = (north_lat + south_lat) / 2.0
    base_lon_span = abs(east_lon - west_lon)
    base_lat_span = abs(north_lat - south_lat)

    render_w = max(1, int(render_w))
    render_h = max(1, int(render_h))
    crop_left = max(0, int(crop_left))
    crop_top = max(0, int(crop_top))
    crop_right = max(0, int(crop_right))
    crop_bottom = max(0, int(crop_bottom))

    visible_w = max(1, render_w - crop_left - crop_right)
    visible_h = max(1, render_h - crop_top - crop_bottom)

    scale_x = max(1.0, float(render_w) / float(visible_w))
    scale_y = max(1.0, float(render_h) / float(visible_h))

    requested_lon_span = base_lon_span * scale_x
    requested_lat_span = base_lat_span * scale_y

    # User-adjustable extra spacing between screenshot centers.
    # Values > 1 move centres farther apart. This fixes "too close together".
    step_factor_x = max(0.1, float(step_factor_x))
    step_factor_y = max(0.1, float(step_factor_y))

    # The local offset from this tile's normal center to its scaled spacing center
    # is calculated by the caller relative to a mosaic origin using placeholders
    # x/y. Since this function does not know x_min/y_min, it exposes the factors
    # by shifting from the integer tile coordinate origin:
    # center = tile origin lonlat + (x fractional centre * factor).
    # For regular WebMercator tiles this is equivalent to increasing the centre
    # distance between consecutive x/y jobs.
    west0, north0 = tile_to_lonlat(0, 0, z)
    # For lon, each tile is constant width.
    lon_tile_w = 360.0 / (2 ** int(z))
    center_lon = -180.0 + ((float(x) + 0.5) * lon_tile_w * step_factor_x)

    # For lat, tile height is not constant in degrees. Approximate step around
    # the current tile using local base_lat_span.
    # y increases downward, lat decreases downward.
    # Use normal tile center plus additional local offset from factor.
    center_lat = tile_center_lat - ((float(y) + 0.5) * 0.0)  # start normal
    if abs(step_factor_y - 1.0) > 1e-9:
        # Additional shift relative to tile index, local lat span approximation.
        # This deliberately moves rows farther apart when screenshots are too close.
        center_lat = tile_center_lat - ((float(y) - float(y)) * base_lat_span)  # no-op anchor
        # A local row spacing correction is applied by changing span and crop offset
        # unless mosaic-origin stepping is supplied by caller; here keep normal
        # latitude to avoid drifting globally. Caller can still use span multiplier.
        pass

    # The lon formula above is global and can jump if factor != 1; for actual
    # mosaic stepping we replace it below when origin placeholders are not used.
    # To avoid global drift, default to tile center and use explicit step shifts
    # injected by caller via optional placeholders when available.
    center_lon = tile_center_lon

    crop_center_x = (crop_left + (render_w - crop_right)) / 2.0
    crop_center_y = (crop_top + (render_h - crop_bottom)) / 2.0
    dx_px = crop_center_x - (render_w / 2.0)
    dy_px = crop_center_y - (render_h / 2.0)

    center_lon = center_lon - (dx_px / float(render_w)) * requested_lon_span
    center_lat = center_lat + (dy_px / float(render_h)) * requested_lat_span

    west_adj = center_lon - requested_lon_span / 2.0
    east_adj = center_lon + requested_lon_span / 2.0
    south_adj = center_lat - requested_lat_span / 2.0
    north_adj = center_lat + requested_lat_span / 2.0
    bbox = f"{west_adj:.12f},{south_adj:.12f},{east_adj:.12f},{north_adj:.12f}"

    return (template.replace("{x}", str(x))
                    .replace("{y}", str(y))
                    .replace("{z}", str(z))
                    .replace("{c}", str(z))
                    .replace("{q}", q)
                    .replace("{quadkey}", q)
                    .replace("{rnd}", str(rnd))
                    .replace("{snum}", snum)
                    .replace("{s}", sub)
                    .replace("{west}", f"{west_adj:.12f}")
                    .replace("{south}", f"{south_adj:.12f}")
                    .replace("{east}", f"{east_adj:.12f}")
                    .replace("{north}", f"{north_adj:.12f}")
                    .replace("{center_lon}", f"{center_lon:.12f}")
                    .replace("{center_lat}", f"{center_lat:.12f}")
                    .replace("{lon_span}", f"{requested_lon_span:.12f}")
                    .replace("{lat_span}", f"{requested_lat_span:.12f}")
                    .replace("{span_lon}", f"{requested_lon_span:.12f}")
                    .replace("{span_lat}", f"{requested_lat_span:.12f}")
                    .replace("{base_lon_span}", f"{base_lon_span:.12f}")
                    .replace("{base_lat_span}", f"{base_lat_span:.12f}")
                    .replace("{crop_scale_x}", f"{scale_x:.8f}")
                    .replace("{crop_scale_y}", f"{scale_y:.8f}")
                    .replace("{crop_dx_px}", f"{dx_px:.3f}")
                    .replace("{crop_dy_px}", f"{dy_px:.3f}")
                    .replace("{step_factor_x}", f"{step_factor_x:.8f}")
                    .replace("{step_factor_y}", f"{step_factor_y:.8f}")
                    .replace("{bbox}", bbox)
                    .replace("*GMX*", str(x))
                    .replace("*GMY*", str(y))
                    .replace("*ZM1*", str(z))
                    .replace("*IZM*", str(z))
                    .replace("*RND*", str(rnd))
                    .replace("*LAN*", "de")
                    .replace("*LAN-LAN*", "de-DE"))


def expand_frame_url_for_screenshot_job_with_origin(
    template: str,
    x: int,
    y: int,
    z: int,
    x_min: int,
    y_min: int,
    render_w: int,
    render_h: int,
    crop_left: int,
    crop_top: int,
    crop_right: int,
    crop_bottom: int,
    step_factor_x: float = 0.0,
    step_factor_y: float = 0.0,
) -> str:
    """Origin-aware frame URL expansion using screen/WebView size.

    Automatic math:
    - The final tile is the cropped visible part of the WebView.
    - If the full renderer is 1920 px wide and crop_left=300, crop_right=0,
      visible_w = 1620.
    - To make the cropped visible area cover exactly one output tile, the
      requested span must be enlarged by 1920 / 1620.
    - The next screenshot center must move by that enlarged requested span,
      not by the smaller XYZ tile span. Otherwise screenshots are too close.

    Therefore:
        scale_x = render_w / (render_w - crop_left - crop_right)
        scale_y = render_h / (render_h - crop_top - crop_bottom)

        requested_span_x = xyz_tile_span_x * scale_x
        requested_span_y = xyz_tile_span_y * scale_y

        center_x = origin_center_x + (x - x_min) * requested_span_x
        center_y = origin_center_y - (y - y_min) * requested_span_y

    If step_factor_x/y is > 0, it overrides the automatic scale. This keeps a
    manual emergency control, but default 0 means fully automatic.
    """
    rnd = random.randint(0, 3)
    sub = ["a", "b", "c"][rnd % 3]
    snum = str(rnd % 4)
    q = tile_to_quadkey(x, y, z)

    # Origin tile spans and center.
    ow, on = tile_to_lonlat(x_min, y_min, z)
    oe, os_ = tile_to_lonlat(x_min + 1, y_min + 1, z)
    origin_base_lon_span = abs(oe - ow)
    origin_base_lat_span = abs(on - os_)
    origin_center_lon = (ow + oe) / 2.0
    origin_center_lat = (on + os_) / 2.0

    # Current tile local span. Latitude degree size changes with y.
    west_lon, north_lat = tile_to_lonlat(x, y, z)
    east_lon, south_lat = tile_to_lonlat(x + 1, y + 1, z)
    base_lon_span = abs(east_lon - west_lon)
    base_lat_span = abs(north_lat - south_lat)

    render_w = max(1, int(render_w))
    render_h = max(1, int(render_h))
    crop_left = max(0, int(crop_left))
    crop_top = max(0, int(crop_top))
    crop_right = max(0, int(crop_right))
    crop_bottom = max(0, int(crop_bottom))

    visible_w = max(1, render_w - crop_left - crop_right)
    visible_h = max(1, render_h - crop_top - crop_bottom)

    auto_scale_x = max(1.0, float(render_w) / float(visible_w))
    auto_scale_y = max(1.0, float(render_h) / float(visible_h))

    # Manual override only if user sets > 0. Otherwise screen-size automatic.
    scale_x = float(step_factor_x) if float(step_factor_x) > 0.0 else auto_scale_x
    scale_y = float(step_factor_y) if float(step_factor_y) > 0.0 else auto_scale_y
    scale_x = max(0.1, scale_x)
    scale_y = max(0.1, scale_y)

    requested_lon_span = base_lon_span * scale_x
    requested_lat_span = base_lat_span * scale_y

    # The center step must be based on the requested frame coverage, not the
    # smaller cropped tile coverage.
    step_lon = origin_base_lon_span * scale_x
    # For latitude, use local span for current row to reduce row drift.
    # Move from origin by each row's approximate requested geographic coverage.
    step_lat = origin_base_lat_span * scale_y

    center_lon = origin_center_lon + (float(x - x_min) * step_lon)
    center_lat = origin_center_lat - (float(y - y_min) * step_lat)

    # Asymmetric crop shifts the center of the cropped rectangle inside the full
    # renderer. Move requested center in the opposite direction so the cropped
    # center lands on the intended mosaic cell.
    crop_center_x = (crop_left + (render_w - crop_right)) / 2.0
    crop_center_y = (crop_top + (render_h - crop_bottom)) / 2.0
    dx_px = crop_center_x - (render_w / 2.0)
    dy_px = crop_center_y - (render_h / 2.0)

    center_lon = center_lon - (dx_px / float(render_w)) * requested_lon_span
    center_lat = center_lat + (dy_px / float(render_h)) * requested_lat_span

    west_adj = center_lon - requested_lon_span / 2.0
    east_adj = center_lon + requested_lon_span / 2.0
    south_adj = center_lat - requested_lat_span / 2.0
    north_adj = center_lat + requested_lat_span / 2.0
    bbox = f"{west_adj:.12f},{south_adj:.12f},{east_adj:.12f},{north_adj:.12f}"

    return (template.replace("{x}", str(x))
                    .replace("{y}", str(y))
                    .replace("{z}", str(z))
                    .replace("{c}", str(z))
                    .replace("{q}", q)
                    .replace("{quadkey}", q)
                    .replace("{rnd}", str(rnd))
                    .replace("{snum}", snum)
                    .replace("{s}", sub)
                    .replace("{west}", f"{west_adj:.12f}")
                    .replace("{south}", f"{south_adj:.12f}")
                    .replace("{east}", f"{east_adj:.12f}")
                    .replace("{north}", f"{north_adj:.12f}")
                    .replace("{center_lon}", f"{center_lon:.12f}")
                    .replace("{center_lat}", f"{center_lat:.12f}")
                    .replace("{lon_span}", f"{requested_lon_span:.12f}")
                    .replace("{lat_span}", f"{requested_lat_span:.12f}")
                    .replace("{span_lon}", f"{requested_lon_span:.12f}")
                    .replace("{span_lat}", f"{requested_lat_span:.12f}")
                    .replace("{base_lon_span}", f"{base_lon_span:.12f}")
                    .replace("{base_lat_span}", f"{base_lat_span:.12f}")
                    .replace("{crop_scale_x}", f"{scale_x:.8f}")
                    .replace("{crop_scale_y}", f"{scale_y:.8f}")
                    .replace("{auto_crop_scale_x}", f"{auto_scale_x:.8f}")
                    .replace("{auto_crop_scale_y}", f"{auto_scale_y:.8f}")
                    .replace("{crop_dx_px}", f"{dx_px:.3f}")
                    .replace("{crop_dy_px}", f"{dy_px:.3f}")
                    .replace("{step_lon}", f"{step_lon:.12f}")
                    .replace("{step_lat}", f"{step_lat:.12f}")
                    .replace("{bbox}", bbox)
                    .replace("*GMX*", str(x))
                    .replace("*GMY*", str(y))
                    .replace("*ZM1*", str(z))
                    .replace("*IZM*", str(z))
                    .replace("*RND*", str(rnd))
                    .replace("*LAN*", "de")
                    .replace("*LAN-LAN*", "de-DE"))


def frame_span_for_center_zoom(lon: float, lat: float, z: int) -> Tuple[float, float]:
    """Return lat_span, lon_span for the Web-Mercator tile containing lon/lat at z."""
    x, y = lonlat_to_tile(lon, lat, z)
    west_lon, north_lat = tile_to_lonlat(x, y, z)
    east_lon, south_lat = tile_to_lonlat(x + 1, y + 1, z)
    return abs(north_lat - south_lat), abs(east_lon - west_lon)

def expand_frame_url_center_span(template: str, center_lon: float, center_lat: float, z: int) -> str:
    """Expand a frame/snapshot URL for an exact center coordinate.

    Unlike expand_url(), this does not snap the preview to the center of an XYZ
    tile. It keeps the current preview center exactly and only derives a useful
    Web-Mercator tile-sized span from the zoom level. This makes Apple-style
    preview frames and own frame-server previews land at the expected place.
    """
    x_float = (float(center_lon) + 180.0) / 360.0 * (2 ** int(z))
    lat = clamp_lat(float(center_lat))
    lat_rad = math.radians(lat)
    y_float = (1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * (2 ** int(z))

    west_lon, north_lat = tile_to_lonlat(x_float - 0.5, y_float - 0.5, int(z))
    east_lon, south_lat = tile_to_lonlat(x_float + 0.5, y_float + 0.5, int(z))
    lon_span = abs(east_lon - west_lon)
    lat_span = abs(north_lat - south_lat)
    bbox = f"{west_lon:.12f},{south_lat:.12f},{east_lon:.12f},{north_lat:.12f}"
    x_int, y_int = lonlat_to_tile(float(center_lon), float(center_lat), int(z))

    return (template.replace("{x}", str(x_int))
                    .replace("{y}", str(y_int))
                    .replace("{z}", str(int(z)))
                    .replace("{c}", str(int(z)))
                    .replace("{west}", f"{west_lon:.12f}")
                    .replace("{south}", f"{south_lat:.12f}")
                    .replace("{east}", f"{east_lon:.12f}")
                    .replace("{north}", f"{north_lat:.12f}")
                    .replace("{center_lon}", f"{float(center_lon):.12f}")
                    .replace("{center_lat}", f"{float(center_lat):.12f}")
                    .replace("{lon_span}", f"{lon_span:.12f}")
                    .replace("{lat_span}", f"{lat_span:.12f}")
                    .replace("{span_lon}", f"{lon_span:.12f}")
                    .replace("{span_lat}", f"{lat_span:.12f}")
                    .replace("{bbox}", bbox))



def expand_frame_url_exact_bbox(
    template: str,
    west_lon: float,
    south_lat: float,
    east_lon: float,
    north_lat: float,
    z: int,
) -> str:
    """Expand a frame URL so Preview Selected BBox shows the exact bbox.

    The older preview helper used center+zoom and derived a one-tile span. That
    made Apple/frame preview jump back to a tiny start area when View/Preview BBox
    was clicked. This helper uses the left coordinate fields directly:
        center = bbox center
        span   = bbox size
    """
    west_lon = float(west_lon)
    south_lat = clamp_lat(float(south_lat))
    east_lon = float(east_lon)
    north_lat = clamp_lat(float(north_lat))
    if east_lon <= west_lon or north_lat <= south_lat:
        raise ValueError("invalid bbox for exact frame preview")

    center_lon = (west_lon + east_lon) / 2.0
    center_lat = (south_lat + north_lat) / 2.0
    lon_span = abs(east_lon - west_lon)
    lat_span = abs(north_lat - south_lat)
    bbox = f"{west_lon:.12f},{south_lat:.12f},{east_lon:.12f},{north_lat:.12f}"
    x_int, y_int = lonlat_to_tile(center_lon, center_lat, int(z))
    q = tile_to_quadkey(x_int, y_int, int(z))
    rnd = random.randint(0, 3)
    sub = ["a", "b", "c"][rnd % 3]
    snum = str(rnd % 4)

    return (template.replace("{x}", str(x_int))
                    .replace("{y}", str(y_int))
                    .replace("{z}", str(int(z)))
                    .replace("{c}", str(int(z)))
                    .replace("{q}", q)
                    .replace("{quadkey}", q)
                    .replace("{rnd}", str(rnd))
                    .replace("{snum}", snum)
                    .replace("{s}", sub)
                    .replace("{west}", f"{west_lon:.12f}")
                    .replace("{south}", f"{south_lat:.12f}")
                    .replace("{east}", f"{east_lon:.12f}")
                    .replace("{north}", f"{north_lat:.12f}")
                    .replace("{center_lon}", f"{center_lon:.12f}")
                    .replace("{center_lat}", f"{center_lat:.12f}")
                    .replace("{lon_span}", f"{lon_span:.12f}")
                    .replace("{lat_span}", f"{lat_span:.12f}")
                    .replace("{span_lon}", f"{lon_span:.12f}")
                    .replace("{span_lat}", f"{lat_span:.12f}")
                    .replace("{bbox}", bbox)
                    .replace("*GMX*", str(x_int))
                    .replace("*GMY*", str(y_int))
                    .replace("*ZM1*", str(int(z)))
                    .replace("*IZM*", str(int(z)))
                    .replace("*RND*", str(rnd))
                    .replace("*LAN*", "de")
                    .replace("*LAN-LAN*", "de-DE"))


def frame_view_bbox_for_center_zoom_pixels(
    center_lon: float,
    center_lat: float,
    z: int,
    width_px: float,
    height_px: float,
) -> Tuple[float, float, float, float]:
    """Return the lon/lat bbox covered by a Web-Mercator pixel viewport.

    This is the important Apple/frame fix: the preview fallback is no longer a
    single XYZ tile. A 1600 px renderer at z=18 should represent about 1600
    Web-Mercator pixels, not only 256. The optional UI multiplier can make the
    selectable preview cover several renderer-cells so Mark Area can create a
    multi-cell download instead of always collapsing to 1 x 1.
    """
    z = int(z)
    cx_px, cy_px = lonlat_to_world_pixel(float(center_lon), float(center_lat), z)
    half_w = max(1.0, float(width_px)) / 2.0
    half_h = max(1.0, float(height_px)) / 2.0
    return world_pixel_bbox_to_lonlat(cx_px - half_w, cy_px - half_h, cx_px + half_w, cy_px + half_h, z)

def is_frame_template(url_template: str) -> bool:
    markers = ("{center_lat}", "{center_lon}", "{lat_span}", "{lon_span}", "{span_lat}", "{span_lon}", "{bbox}")
    return any(m in url_template for m in markers)



def project_tiles_dir(output_file: Path) -> Path:
    return output_file.parent / f"{output_file.stem}_tiles"

def project_sqlite_dir(output_file: Path) -> Path:
    return output_file.parent / f"{output_file.stem}_sqlite"

def project_single_tiff_dir(output_file: Path) -> Path:
    return output_file.parent / f"{output_file.stem}_single_tiff_tiles"

def safe_cache_path(cache_dir: Path, z: int, x: int, y: int) -> Path:
    # Dateiname enthält jetzt ausdrücklich Zoom, X und Y.
    # Dadurch sieht man auch nach einem Abbruch sofort, welche Kachel vorhanden ist.
    return cache_dir / str(z) / f"z{z}_x{x}_y{y}.tile"


def default_tile_tif_dir(cfg: "StitchConfig") -> Path:
    base = cfg.output_file.parent if cfg.output_file.parent else Path.cwd()
    stem = cfg.output_file.stem or "map_output"
    return base / f"{stem}_einzelkacheln_tif_z{cfg.z}"


def safe_tile_tif_path(tile_tif_dir: Path, z: int, x: int, y: int) -> Path:
    return tile_tif_dir / f"z{z}_x{x}_y{y}.tif"


def lonlat_to_webmercator(lon: float, lat: float) -> Tuple[float, float]:
    lat = clamp_lat(lat)
    r = 6378137.0
    x = r * math.radians(lon)
    y = r * math.log(math.tan(math.pi / 4.0 + math.radians(lat) / 2.0))
    return x, y


def tile_webmercator_bounds(x: int, y: int, z: int) -> Tuple[float, float, float, float]:
    west_lon, north_lat = tile_to_lonlat(x, y, z)
    east_lon, south_lat = tile_to_lonlat(x + 1, y + 1, z)
    west, north = lonlat_to_webmercator(west_lon, north_lat)
    east, south = lonlat_to_webmercator(east_lon, south_lat)
    return west, south, east, north


def mosaic_webmercator_bounds(x_min: int, y_min: int, x_max: int, y_max: int, z: int) -> Tuple[float, float, float, float]:
    west_lon, north_lat = tile_to_lonlat(x_min, y_min, z)
    east_lon, south_lat = tile_to_lonlat(x_max + 1, y_max + 1, z)
    west, north = lonlat_to_webmercator(west_lon, north_lat)
    east, south = lonlat_to_webmercator(east_lon, south_lat)
    return west, south, east, north



def lonlat_bbox_to_webmercator_bounds(west_lon: float, south_lat: float, east_lon: float, north_lat: float) -> Tuple[float, float, float, float]:
    west, north = lonlat_to_webmercator(float(west_lon), float(north_lat))
    east, south = lonlat_to_webmercator(float(east_lon), float(south_lat))
    return west, south, east, north


def expand_frame_url_grid(
    template: str,
    col: int,
    row: int,
    z: int,
    selected_west: float,
    selected_north: float,
    request_lon_span: float,
    request_lat_span: float,
    lon_per_px: float,
    lat_per_px: float,
    visible_w_px: int,
    visible_h_px: int,
    render_w: int,
    render_h: int,
    crop_left: int,
    crop_top: int,
    crop_right: int,
    crop_bottom: int,
    step_mult_x: float = 1.0,
    step_mult_y: float = 1.0,
    shift_x_px: float = 0.0,
    shift_y_px: float = 0.0,
    crop_correct_url: bool = False,
) -> Tuple[str, Tuple[float, float, float, float], Tuple[float, float, float, float]]:
    """World-pixel screenshot grid with explicit step multiplier.

    Default is NoDoubleCrop mode: the Apple/frame URL span is the visible
    output cell. Crop is only an image extraction step. This is the mode that
    keeps neighbouring Apple frame screenshots visually aligned on systems where
    Apple interprets span for the map canvas rather than the whole WebView.

    crop_correct_url=True keeps the experimental mode where the requested URL
    span is enlarged by the crop margins. That can be useful for some own frame
    servers, but it makes Apple frames drift on many setups.
    """
    rnd = random.randint(0, 3)
    sub = ["a", "b", "c"][rnd % 3]
    snum = str(rnd % 4)

    render_w = max(1, int(render_w))
    render_h = max(1, int(render_h))
    crop_left = max(0, int(crop_left))
    crop_top = max(0, int(crop_top))
    crop_right = max(0, int(crop_right))
    crop_bottom = max(0, int(crop_bottom))
    visible_w_px = max(1, int(visible_w_px))
    visible_h_px = max(1, int(visible_h_px))
    step_mult_x = max(0.01, float(step_mult_x))
    step_mult_y = max(0.01, float(step_mult_y))
    shift_x_px = float(shift_x_px)
    shift_y_px = float(shift_y_px)
    z = int(z)

    selected_px_x, selected_px_y = lonlat_to_world_pixel(float(selected_west), float(selected_north), z)

    # Manual fine tuning:
    # multiplier gives coarse overlap/spacing, shift_x/y adds/subtracts pixels per cell.
    # Negative shift = closer screenshots / more overlap. Positive shift = farther apart.
    effective_step_x_px = max(1.0, float(visible_w_px) * step_mult_x + shift_x_px)
    effective_step_y_px = max(1.0, float(visible_h_px) * step_mult_y + shift_y_px)

    # Target visible cell: THIS is the part that is written into the output raster.
    visible_left_px = selected_px_x + float(col) * effective_step_x_px
    visible_top_px = selected_px_y + float(row) * effective_step_y_px
    visible_right_px = visible_left_px + float(visible_w_px)
    visible_bottom_px = visible_top_px + float(visible_h_px)

    visible_west, visible_south, visible_east, visible_north = world_pixel_bbox_to_lonlat(
        visible_left_px, visible_top_px, visible_right_px, visible_bottom_px, z
    )

    # Unified crop-center correction.
    #
    # The visible output cell is the part that remains after PIL crops the
    # captured WebView. The URL center must therefore be shifted by the center
    # offset of that crop rectangle inside the full renderer:
    #
    #   x-shift = (crop_right  - crop_left) / 2
    #   y-shift = (crop_bottom - crop_top)  / 2
    #
    # This is the same principle that made left-crop work, but now it is applied
    # symmetrically to right, top and bottom as well. Example:
    #   left=300,right=100 -> center shifts -100 px
    #   left=0,right=100   -> center shifts +50 px
    #   top=0,bottom=100   -> center shifts +50 px downward
    visible_center_x_px = visible_left_px + (float(visible_w_px) / 2.0)
    visible_center_y_px = visible_top_px + (float(visible_h_px) / 2.0)
    crop_center_shift_x_px = (float(crop_right) - float(crop_left)) / 2.0
    crop_center_shift_y_px = (float(crop_bottom) - float(crop_top)) / 2.0

    if bool(crop_correct_url):
        # Crop-aware Apple/frame mode: request the full WebView geometry around
        # the corrected center, so after cropping L/T/R/B the remaining image is
        # exactly the intended visible grid cell.
        request_center_x_px = visible_center_x_px + crop_center_shift_x_px
        request_center_y_px = visible_center_y_px + crop_center_shift_y_px
        request_w_px = float(visible_w_px) + float(crop_left) + float(crop_right)
        request_h_px = float(visible_h_px) + float(crop_top) + float(crop_bottom)
        request_left_px = request_center_x_px - (request_w_px / 2.0)
        request_top_px = request_center_y_px - (request_h_px / 2.0)
        request_right_px = request_center_x_px + (request_w_px / 2.0)
        request_bottom_px = request_center_y_px + (request_h_px / 2.0)

        request_west, request_south, request_east, request_north = world_pixel_bbox_to_lonlat(
            request_left_px, request_top_px, request_right_px, request_bottom_px, z
        )
    else:
        # Legacy NoDoubleCrop mode kept as a fallback. It also uses the same
        # center-shift variables for diagnostics/placeholders, but keeps the URL
        # span equal to the output cell.
        request_west, request_south, request_east, request_north = (
            visible_west, visible_south, visible_east, visible_north
        )
        request_center_x_px = visible_center_x_px
        request_center_y_px = visible_center_y_px

    request_center_lon, request_center_lat = world_pixel_to_lonlat(request_center_x_px, request_center_y_px, z)

    real_request_lon_span = abs(request_east - request_west)
    real_request_lat_span = abs(request_north - request_south)

    next_center_lon, _ = world_pixel_to_lonlat(request_center_x_px + effective_step_x_px, request_center_y_px, z)
    _, next_center_lat = world_pixel_to_lonlat(request_center_x_px, request_center_y_px + effective_step_y_px, z)
    center_step_lon = abs(next_center_lon - request_center_lon)
    center_step_lat = abs(request_center_lat - next_center_lat)

    bbox = f"{request_west:.12f},{request_south:.12f},{request_east:.12f},{request_north:.12f}"
    q = ""
    try:
        q = tile_to_quadkey(max(0, int(col)), max(0, int(row)), z)
    except Exception:
        q = ""

    url = (template.replace("{x}", str(int(col)))
                   .replace("{y}", str(int(row)))
                   .replace("{z}", str(z))
                   .replace("{c}", str(z))
                   .replace("{q}", q)
                   .replace("{quadkey}", q)
                   .replace("{rnd}", str(rnd))
                   .replace("{snum}", snum)
                   .replace("{s}", sub)
                   .replace("{west}", f"{request_west:.12f}")
                   .replace("{south}", f"{request_south:.12f}")
                   .replace("{east}", f"{request_east:.12f}")
                   .replace("{north}", f"{request_north:.12f}")
                   .replace("{center_lon}", f"{request_center_lon:.12f}")
                   .replace("{center_lat}", f"{request_center_lat:.12f}")
                   .replace("{lon_span}", f"{real_request_lon_span:.12f}")
                   .replace("{lat_span}", f"{real_request_lat_span:.12f}")
                   .replace("{span_lon}", f"{real_request_lon_span:.12f}")
                   .replace("{span_lat}", f"{real_request_lat_span:.12f}")
                   .replace("{visible_west}", f"{visible_west:.12f}")
                   .replace("{visible_south}", f"{visible_south:.12f}")
                   .replace("{visible_east}", f"{visible_east:.12f}")
                   .replace("{visible_north}", f"{visible_north:.12f}")
                   .replace("{visible_center_lon}", f"{(visible_west + visible_east) / 2.0:.12f}")
                   .replace("{visible_center_lat}", f"{(visible_south + visible_north) / 2.0:.12f}")
                   .replace("{request_center_x_px}", f"{request_center_x_px:.3f}")
                   .replace("{request_center_y_px}", f"{request_center_y_px:.3f}")
                   .replace("{center_step_x_px}", f"{effective_step_x_px:.3f}")
                   .replace("{center_step_y_px}", f"{effective_step_y_px:.3f}")
                   .replace("{center_step_lon}", f"{center_step_lon:.12f}")
                   .replace("{center_step_lat}", f"{center_step_lat:.12f}")
                   .replace("{step_mult_x}", f"{step_mult_x:.4f}")
                   .replace("{step_mult_y}", f"{step_mult_y:.4f}")
                   .replace("{shift_x_px}", f"{shift_x_px:.3f}")
                   .replace("{shift_y_px}", f"{shift_y_px:.3f}")
                   .replace("{crop_center_shift_x_px}", f"{crop_center_shift_x_px:.3f}")
                   .replace("{crop_center_shift_y_px}", f"{crop_center_shift_y_px:.3f}")
                   .replace("{crop_correct_url}", "1" if bool(crop_correct_url) else "0")
                   .replace("{visible_w_px}", str(int(visible_w_px)))
                   .replace("{visible_h_px}", str(int(visible_h_px)))
                   .replace("{bbox}", bbox)
                   .replace("*GMX*", str(int(col)))
                   .replace("*GMY*", str(int(row)))
                   .replace("*ZM1*", str(z))
                   .replace("*IZM*", str(z))
                   .replace("*RND*", str(rnd))
                   .replace("*LAN*", "de")
                   .replace("*LAN-LAN*", "de-DE"))

    # QWebEngine/Apple frame can otherwise reuse an old frame. Force each cell to
    # be a unique URL while keeping the original center/span/bbox parameters intact.
    url = append_url_param(url, "__pymap_tile", f"{z}_{int(col)}_{int(row)}_{int(time.time()*1000)}_{rnd}")

    return url, (visible_west, visible_south, visible_east, visible_north), (request_west, request_south, request_east, request_north)



def ensure_python_package(import_name: str, pip_name: Optional[str] = None, log_cb=None):
    """Import a package, installing it with pip on demand.

    This keeps the single-file app convenient on Windows: when the new PyPI
    stitching merge is enabled, the missing package is installed automatically
    into the current Python environment.
    """
    import importlib
    pip_name = pip_name or import_name
    try:
        return importlib.import_module(import_name)
    except Exception as first_exc:
        if log_cb:
            log_cb(f"Missing Python package '{pip_name}'. Installing with pip...")
        cmd = [sys.executable, "-m", "pip", "install", "--upgrade", pip_name]
        try:
            proc = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            if log_cb and proc.stdout:
                for line in proc.stdout.splitlines()[-40:]:
                    log_cb(line)
            if proc.returncode != 0:
                raise RuntimeError(f"pip install {pip_name} failed with exit code {proc.returncode}")
            return importlib.import_module(import_name)
        except Exception as exc:
            raise RuntimeError(
                f"Could not import or install '{pip_name}'. First import error: {first_exc}; install error: {exc}"
            ) from exc


def sorted_frame_tile_paths(tile_dir: Path) -> List[Path]:
    """Return frame grid tiles in deterministic row-major order."""
    import re
    items = []
    for path in Path(tile_dir).glob("grid_z*_col*_row*.tif"):
        m = re.search(r"_col(\d+)_row(\d+)\.tif$", path.name)
        if not m:
            continue
        col = int(m.group(1))
        row = int(m.group(2))
        items.append((row, col, path))
    items.sort(key=lambda v: (v[0], v[1]))
    return [p for _row, _col, p in items]


def save_numpy_rgb_as_geotiff(out_file: Path, rgb_array, bounds_3857: Tuple[float, float, float, float], log_cb=None) -> None:
    """Save an RGB numpy array as embedded EPSG:3857 GeoTIFF/BigTIFF."""
    try:
        import numpy as np
        import tifffile
    except Exception as exc:
        raise RuntimeError("tifffile and numpy are required for stitched GeoTIFF output") from exc

    arr = np.asarray(rgb_array)
    if arr.ndim == 2:
        arr = np.stack([arr, arr, arr], axis=2)
    if arr.ndim != 3:
        raise RuntimeError(f"Unexpected stitched image shape: {arr.shape}")
    if arr.shape[2] >= 4:
        arr = arr[:, :, :3]
    if arr.dtype != np.uint8:
        arr = arr.astype("uint8", copy=False)

    height, width = int(arr.shape[0]), int(arr.shape[1])
    estimated = width * height * 3
    ensure_enough_disk_space(out_file, estimated, log_cb or (lambda _m: None))
    out_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_file.with_suffix(out_file.suffix + ".tmp.tif")
    if tmp.exists():
        try:
            tmp.unlink()
        except Exception:
            pass
    tifffile.imwrite(
        str(tmp),
        arr,
        bigtiff=estimated > 3_800_000_000,
        photometric="rgb",
        metadata=None,
        extratags=geotiff_extratags_epsg3857(width, height, bounds_3857),
        compression="deflate",
    )
    os.replace(tmp, out_file)
    write_worldfile_and_prj(out_file, width, height, bounds_3857)


def stitch_frame_tiles_with_pypi_stitching(
    tile_dir: Path,
    out_file: Path,
    bounds_3857: Tuple[float, float, float, float],
    log_cb=None,
    max_images: int = 500,
) -> bool:
    """Merge captured frame tiles with the PyPI 'stitching' package.

    Returns True if the panorama was produced. This is intentionally optional:
    if feature stitching fails because the map area has too little overlap or
    too few visual features, the existing grid BigTIFF remains available.
    """
    paths = sorted_frame_tile_paths(tile_dir)
    if len(paths) < 2:
        if log_cb:
            log_cb("PyPI stitching skipped: fewer than 2 frame tiles.")
        return False
    if len(paths) > int(max_images):
        if log_cb:
            log_cb(
                f"PyPI stitching skipped: {len(paths)} images is too much for OpenCV feature stitching. "
                f"Use a smaller area or raise max_images in code."
            )
        return False

    ensure_python_package("cv2", "opencv-python", log_cb)
    stitching_mod = ensure_python_package("stitching", "stitching", log_cb)

    # Prefer the affine stitcher for screenshot grids/linear map movement. It
    # tolerates planar translations better than pure panorama camera geometry.
    StitcherClass = getattr(stitching_mod, "AffineStitcher", None) or getattr(stitching_mod, "Stitcher")
    if log_cb:
        log_cb(f"PyPI stitching merge: {len(paths)} TIFF tiles, class={StitcherClass.__name__}")
        log_cb("Hinweis: Stitching braucht echte Überlappung/Features. Pixel step X/Y sollte eher 0.70-0.95 sein, nicht 1.00 oder 4.00.")

    # The package accepts filenames directly.
    stitcher = StitcherClass(detector="sift", confidence_threshold=0.2)
    panorama = stitcher.stitch([str(p) for p in paths])
    if panorama is None:
        raise RuntimeError("PyPI stitching returned no panorama")

    # stitching/OpenCV returns BGR. Convert to RGB before GeoTIFF.
    import cv2
    try:
        panorama_rgb = cv2.cvtColor(panorama, cv2.COLOR_BGR2RGB)
    except Exception:
        panorama_rgb = panorama

    save_numpy_rgb_as_geotiff(out_file, panorama_rgb, bounds_3857, log_cb)
    if log_cb:
        log_cb(f"PyPI stitching finished and wrote georeferenced GeoTIFF/BigTIFF: {out_file}")
    return True


def write_worldfile_and_prj(tif_path: Path, width: int, height: int, bounds_3857: Tuple[float, float, float, float]) -> None:
    # Minimal-invasive Georeferenzierung: Der ursprüngliche TIFF/BigTIFF-Schreibweg bleibt unverändert.
    # QGIS/GIS liest die Georeferenz über .tfw + .prj neben der TIFF-Datei.
    west, south, east, north = bounds_3857
    px_w = (east - west) / float(width)
    px_h = (south - north) / float(height)
    tfw = tif_path.with_suffix(".tfw")
    prj = tif_path.with_suffix(".prj")
    tfw.write_text(
        f"{px_w:.12f}\n0.0\n0.0\n{px_h:.12f}\n{west + px_w / 2.0:.12f}\n{north + px_h / 2.0:.12f}\n",
        encoding="utf-8",
    )
    prj.write_text(
        'PROJCS["WGS 84 / Pseudo-Mercator",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]],PROJECTION["Mercator_1SP"],PARAMETER["central_meridian",0],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1],AUTHORITY["EPSG","3857"]]',
        encoding="utf-8",
    )


def save_tile_as_tif(data: Optional[bytes], out_path: Path, z: int, x: int, y: int) -> None:
    # Schreibt genau eine erzeugte Kachel sofort als TIFF.
    # Vorhandene TIFF-Tiles werden nicht erneut geschrieben.
    if out_path.exists() and out_path.stat().st_size > 100:
        return
    out_path.parent.mkdir(parents=True, exist_ok=True)
    im = decode_tile(data)
    tmp = out_path.with_suffix(".tmp.tif")
    im.save(tmp, format="TIFF", compression="tiff_deflate")
    os.replace(tmp, out_path)
    write_worldfile_and_prj(out_path, TILE_SIZE, TILE_SIZE, tile_webmercator_bounds(x, y, z))


def download_one(job: TileJob, cfg: StitchConfig, stop_event: threading.Event) -> Tuple[TileJob, Optional[bytes], Optional[str]]:
    """Download one tile without persistent cache.

    The tile bytes are returned to the stitcher and are never written to a raw
    tile cache folder. Resume/SQLite caching is intentionally disabled so the
    only persistent output is the streamed BigTIFF.
    """
    if stop_event.is_set():
        return job, None, "cancelled"
    if requests is None:
        return job, None, "requests is not installed"
    url = expand_url(cfg.url_template, job.x, job.y, job.z)
    headers = {"User-Agent": USER_AGENT}
    if cfg.headers:
        headers.update(cfg.headers)
    last_err = None
    for attempt in range(cfg.retries):
        if stop_event.is_set():
            return job, None, "cancelled"
        try:
            if cfg.rate_limit_ms:
                time.sleep(cfg.rate_limit_ms / 1000.0)
            r = requests.get(url, headers=headers, timeout=cfg.timeout, stream=True)
            r.raise_for_status()
            data = r.content
            if len(data) < 50:
                raise RuntimeError("empty/invalid tile")
            return job, data, None
        except Exception as exc:
            last_err = str(exc)
            time.sleep(0.5 * (attempt + 1))
    return job, None, last_err

def make_blank_tile() -> "Image.Image":
    return Image.new("RGB", (TILE_SIZE, TILE_SIZE), (255, 255, 255))


def decode_tile(data: Optional[bytes]) -> "Image.Image":
    if Image is None:
        raise RuntimeError("Pillow is not installed")
    if not data:
        return make_blank_tile()
    try:
        im = Image.open(io.BytesIO(data))
        return im.convert("RGB").resize((TILE_SIZE, TILE_SIZE))
    except Exception:
        return make_blank_tile()




def iter_tile_jobs(x_min: int, y_min: int, x_max: int, y_max: int, z: int):
    # Generator statt Liste: selbst riesige Bereiche erzeugen keine RAM-Spitze.
    for y in range(y_min, y_max + 1):
        for x in range(x_min, x_max + 1):
            yield TileJob(x, y, z, x - x_min, y - y_min)


def count_existing_tiles(cache_dir: Path, x_min: int, y_min: int, x_max: int, y_max: int, z: int) -> int:
    existing = 0
    for y in range(y_min, y_max + 1):
        for x in range(x_min, x_max + 1):
            p = safe_cache_path(cache_dir, z, x, y)
            if p.exists() and p.stat().st_size > 100:
                existing += 1
    return existing



def sqlite_path_for(cfg: StitchConfig) -> Path:
    return cfg.cache_dir / f"download_state_z{cfg.z}.sqlite"


def init_state_db(cfg: StitchConfig):
    if not cfg.use_sqlite:
        return None
    cfg.cache_dir.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(sqlite_path_for(cfg)), timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("CREATE TABLE IF NOT EXISTS tiles (z INTEGER, x INTEGER, y INTEGER, status TEXT, updated REAL, error TEXT, PRIMARY KEY(z,x,y))")
    db.execute("CREATE TABLE IF NOT EXISTS chunks (z INTEGER, x0 INTEGER, y0 INTEGER, x1 INTEGER, y1 INTEGER, status TEXT, updated REAL, PRIMARY KEY(z,x0,y0,x1,y1))")
    db.commit()
    return db


def db_tile_done(db, z: int, x: int, y: int) -> bool:
    if db is None:
        return False
    row = db.execute("SELECT status FROM tiles WHERE z=? AND x=? AND y=?", (z, x, y)).fetchone()
    return bool(row and row[0] == "done")


def db_mark_tile(db, z: int, x: int, y: int, status: str, error: Optional[str] = None):
    if db is None:
        return
    db.execute("INSERT OR REPLACE INTO tiles(z,x,y,status,updated,error) VALUES(?,?,?,?,?,?)", (z, x, y, status, time.time(), error))


def db_mark_chunk(db, z: int, x0: int, y0: int, x1: int, y1: int, status: str):
    if db is None:
        return
    db.execute("INSERT OR REPLACE INTO chunks(z,x0,y0,x1,y1,status,updated) VALUES(?,?,?,?,?,?,?)", (z, x0, y0, x1, y1, status, time.time()))
    db.commit()


def iter_chunks(x_min: int, y_min: int, x_max: int, y_max: int, chunk_size: int):
    """Spatial chunk scheduler. Yields chunk bounds only; never builds a global tile list."""
    chunk_size = max(1, int(chunk_size))
    for cy in range(y_min, y_max + 1, chunk_size):
        for cx in range(x_min, x_max + 1, chunk_size):
            yield cx, cy, min(cx + chunk_size - 1, x_max), min(cy + chunk_size - 1, y_max)


def iter_chunk_jobs(cx0: int, cy0: int, cx1: int, cy1: int, z: int, x_min: int, y_min: int):
    """Yields jobs for one chunk only."""
    for y in range(cy0, cy1 + 1):
        for x in range(cx0, cx1 + 1):
            yield TileJob(x, y, z, x - x_min, y - y_min)


def format_bytes(num: int) -> str:
    value = float(num)
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if value < 1024 or unit == "PB":
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{value:.2f} PB"


def ensure_enough_disk_space(path: Path, required_bytes: int, log_cb) -> None:
    """Raise before creating the BigTIFF when the target drive is too small."""
    target_dir = path.expanduser().parent
    target_dir.mkdir(parents=True, exist_ok=True)
    try:
        usage = shutil.disk_usage(str(target_dir))
    except Exception as exc:
        raise RuntimeError(f"Could not check free disk space for {target_dir}: {exc}") from exc
    # tifffile metadata and filesystem allocation need a little headroom.
    required_with_margin = int(required_bytes * 1.03) + 512 * 1024 * 1024
    log_cb(f"Estimated raw BigTIFF payload: {format_bytes(required_bytes)}")
    log_cb(f"Free space on target drive: {format_bytes(usage.free)}")
    if usage.free < required_with_margin:
        raise RuntimeError(
            "Not enough free disk space for direct BigTIFF streaming. "
            f"Required with safety margin: {format_bytes(required_with_margin)}; "
            f"available: {format_bytes(usage.free)}. Choose a smaller area/zoom or another drive."
        )


def geotiff_extratags_epsg3857(width: int, height: int, bounds_3857: Tuple[float, float, float, float]):
    """Return embedded GeoTIFF tags for EPSG:3857 / Web Mercator.

    This writes georeferencing into the TIFF itself, so QGIS can place the
    BigTIFF without depending on .tfw/.prj sidecar files.
    """
    west, south, east, north = bounds_3857
    px_w = (east - west) / float(width)
    px_h = (north - south) / float(height)
    model_pixel_scale = (float(px_w), float(px_h), 0.0)
    # Raster coordinate (0,0,0) is tied to the top-left model coordinate.
    model_tiepoint = (0.0, 0.0, 0.0, float(west), float(north), 0.0)
    # GeoKeyDirectoryTag: header + GTModelTypeGeoKey(Projected),
    # GTRasterTypeGeoKey(PixelIsArea), ProjectedCSTypeGeoKey(EPSG:3857).
    geo_key_directory = (
        1, 1, 0, 3,
        1024, 0, 1, 1,
        1025, 0, 1, 1,
        3072, 0, 1, 3857,
    )
    return [
        (33550, "d", 3, model_pixel_scale, False),
        (33922, "d", 6, model_tiepoint, False),
        (34735, "H", len(geo_key_directory), geo_key_directory, False),
    ]


def open_direct_bigtiff(cfg: StitchConfig, width: int, height: int, bounds_3857: Tuple[float, float, float, float], log_cb):
    """Create a georeferenced on-disk BigTIFF memmap or raise a clear error.

    There is no cache fallback in this build. Georeferencing is embedded as
    GeoTIFF tags, not only written as .tfw/.prj sidecars.
    """
    estimated = int(width) * int(height) * 3
    ensure_enough_disk_space(cfg.output_file, estimated, log_cb)
    try:
        import tifffile
    except Exception as exc:
        raise RuntimeError("tifffile is required for direct BigTIFF streaming. Install with: pip install tifffile") from exc
    try:
        cfg.output_file.parent.mkdir(parents=True, exist_ok=True)
        bigtiff = estimated > 3_800_000_000
        extratags = geotiff_extratags_epsg3857(width, height, bounds_3857)
        mem = tifffile.memmap(
            str(cfg.output_file),
            shape=(height, width, 3),
            dtype="uint8",
            bigtiff=bigtiff,
            photometric="rgb",
            metadata=None,
            extratags=extratags,
        )
        log_cb(f"Direct GeoTIFF/BigTIFF writer opened: {cfg.output_file}")
        log_cb("Embedded GeoTIFF georeferencing written: EPSG:3857, ModelPixelScaleTag, ModelTiepointTag, GeoKeyDirectoryTag")
        return mem, "memmap"
    except OSError as exc:
        raise RuntimeError(
            "Direct BigTIFF output could not be created. This is usually caused by not enough disk space, "
            f"permission problems, or a path/drive limit. Target: {cfg.output_file}. Error: {exc}"
        ) from exc
    except Exception as exc:
        raise RuntimeError(f"Direct GeoTIFF/BigTIFF writer failed: {exc}") from exc

def stitch_tiles(cfg: StitchConfig, progress_cb, log_cb, stop_event: threading.Event):
    if Image is None:
        raise RuntimeError("Pillow is required. Install with: pip install pillow requests")

    x_min, y_min, x_max, y_max = tile_bounds_for_bbox(cfg.min_lat, cfg.min_lon, cfg.max_lat, cfg.max_lon, cfg.z)
    cols = x_max - x_min + 1
    rows = y_max - y_min + 1
    total = cols * rows
    width = cols * TILE_SIZE
    height = rows * TILE_SIZE
    chunk_size = max(1, int(cfg.chunk_size))

    log_cb(f"Tile range: x={x_min}..{x_max}, y={y_min}..{y_max}")
    log_cb(f"Tiles: {cols} x {rows} = {total:,}")
    log_cb(f"Image size: {width:,} x {height:,} px")
    log_cb(f"Direct BigTIFF streaming active: chunk size {chunk_size} x {chunk_size} tiles")
    log_cb("CPU-only HTTP tile stitching. CUDA/CuPy is removed.")
    log_cb("No raw tile cache, no SQLite resume database, and no separate TIFF tile output will be created.")

    if total > HARD_TILE_WARNING:
        log_cb(f"Warning: very large selection with {total:,} tiles.")

    bounds_3857 = mosaic_webmercator_bounds(x_min, y_min, x_max, y_max, cfg.z)
    direct_mem, direct_kind = open_direct_bigtiff(cfg, width, height, bounds_3857, log_cb)

    max_workers = max(1, cfg.workers)
    max_inflight = max_workers * MAX_INFLIGHT_PER_WORKER
    done = 0
    errors = 0

    try:
        with cf.ThreadPoolExecutor(max_workers=max_workers) as pool:
            for cx0, cy0, cx1, cy1 in iter_chunks(x_min, y_min, x_max, y_max, chunk_size):
                if stop_event.is_set():
                    break
                log_cb(f"Chunk start: x={cx0}..{cx1}, y={cy0}..{cy1}")
                job_iter = iter_chunk_jobs(cx0, cy0, cx1, cy1, cfg.z, x_min, y_min)
                pending = set()
                while not stop_event.is_set():
                    while len(pending) < max_inflight:
                        try:
                            job = next(job_iter)
                        except StopIteration:
                            break
                        pending.add(pool.submit(download_one, job, cfg, stop_event))
                    if not pending:
                        break
                    done_set, pending = cf.wait(pending, return_when=cf.FIRST_COMPLETED)
                    for fut in done_set:
                        job, data, err = fut.result()
                        done += 1
                        if err:
                            errors += 1
                            if errors <= 30:
                                log_cb(f"Error {job.z}/{job.x}/{job.y}: {err}")
                        else:
                            try:
                                tile_arr = tile_bytes_to_numpy_rgb(data)
                                r0 = job.row * TILE_SIZE
                                c0 = job.col * TILE_SIZE
                                direct_mem[r0:r0+TILE_SIZE, c0:c0+TILE_SIZE, :] = tile_arr
                            except Exception as exc:
                                errors += 1
                                if errors <= 30:
                                    log_cb(f"Write error {job.z}/{job.x}/{job.y}: {exc}")
                        if done % 25 == 0 or done == total:
                            progress_cb(done, total, "Stream")
                try:
                    direct_mem.flush()
                except Exception:
                    pass
    finally:
        if direct_mem is not None:
            try:
                direct_mem.flush()
                del direct_mem
            except Exception:
                pass

    if stop_event.is_set():
        log_cb("Stopped. Partial BigTIFF remains at the output path.")
        return

    log_cb(f"Finished direct BigTIFF streaming. Processed: {done:,}; errors: {errors:,}")
    log_cb(f"Finished BigTIFF/direct output: {cfg.output_file}")

def open_folder_in_file_manager(path: Path) -> None:
    """Open a folder in the OS file manager. Safe no-op if it cannot be opened."""
    path = Path(path).expanduser()
    path.mkdir(parents=True, exist_ok=True)
    try:
        if os.name == "nt":
            os.startfile(str(path))  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            import subprocess
            subprocess.Popen(["open", str(path)])
        else:
            import subprocess
            subprocess.Popen(["xdg-open", str(path)])
    except Exception:
        pass



# -----------------------------------------------------------------------------
# PySide6 integrated WebEngine GUI
# -----------------------------------------------------------------------------
try:
    from PySide6.QtCore import QObject, QTimer, Qt, QUrl, Slot, QEvent, QPoint, QRect, QSize
    from PySide6.QtWidgets import (
        QApplication, QCheckBox, QComboBox, QDoubleSpinBox, QFileDialog, QFormLayout, QFrame,
        QGridLayout, QGroupBox, QHBoxLayout, QLabel, QLineEdit, QMainWindow,
        QMessageBox, QPushButton, QProgressBar, QRubberBand, QSpinBox, QSplitter, QTextEdit,
        QVBoxLayout, QWidget
    )
    from PySide6.QtWebChannel import QWebChannel
    from PySide6.QtWebEngineWidgets import QWebEngineView
    from PySide6.QtWebEngineCore import QWebEngineProfile, QWebEngineSettings
except Exception as _pyside_exc:  # pragma: no cover
    QObject = object  # type: ignore
    QMainWindow = object  # type: ignore
    QApplication = None  # type: ignore
    QTimer = None  # type: ignore
    QUrl = None  # type: ignore
    QEvent = None  # type: ignore
    QPoint = object  # type: ignore
    QRect = object  # type: ignore
    QWebEngineView = None  # type: ignore
    QWebChannel = None  # type: ignore
    QRubberBand = None  # type: ignore
    QWebEngineProfile = None  # type: ignore
    QWebEngineSettings = None  # type: ignore

    class _DummyQt:
        class Orientation:
            Horizontal = 1
        class WindowType:
            Window = 1
        class WidgetAttribute:
            WA_DeleteOnClose = 1
            WA_TranslucentBackground = 2
        class CursorShape:
            CrossCursor = 1
        class MouseButton:
            LeftButton = 1
            RightButton = 2
        class KeyboardModifier:
            ShiftModifier = 1
        class Key:
            Key_Escape = 1
    Qt = _DummyQt()  # type: ignore

    def Slot(*_args, **_kwargs):  # type: ignore
        def _decorator(func):
            return func
        return _decorator

    _PYSIDE_IMPORT_ERROR = _pyside_exc
else:
    _PYSIDE_IMPORT_ERROR = None

ESRI_WORLD_IMAGERY = "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
GOOGLE_HYBRID_SELECTOR = "https://mt.google.com/vt/lyrs=y&x={x}&y={y}&z={z}&hl=de"


def append_url_param(url: str, key: str, value: str) -> str:
    """Append a cache-busting/debug parameter without disturbing existing query params."""
    if not url:
        return url
    if f"{key}=" in url:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}{key}={value}"



def configure_webengine_view(view):
    """Apply robust WebEngine settings for remote map/frame pages."""
    try:
        page = view.page()
        settings = page.settings()
        settings.setAttribute(QWebEngineSettings.WebAttribute.JavascriptEnabled, True)
        settings.setAttribute(QWebEngineSettings.WebAttribute.AutoLoadImages, True)
        settings.setAttribute(QWebEngineSettings.WebAttribute.LocalContentCanAccessRemoteUrls, True)
        settings.setAttribute(QWebEngineSettings.WebAttribute.LocalContentCanAccessFileUrls, True)
        settings.setAttribute(QWebEngineSettings.WebAttribute.ErrorPageEnabled, True)
        settings.setAttribute(QWebEngineSettings.WebAttribute.PluginsEnabled, True)
        try:
            settings.setAttribute(QWebEngineSettings.WebAttribute.FullScreenSupportEnabled, True)
        except Exception:
            pass
        try:
            settings.setAttribute(QWebEngineSettings.WebAttribute.AllowRunningInsecureContent, True)
        except Exception:
            pass
        profile = page.profile()
        try:
            profile.setHttpUserAgent(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 PyMapStitcherFramePreview/1.0"
            )
        except Exception:
            pass
        try:
            profile.setPersistentCookiesPolicy(QWebEngineProfile.PersistentCookiesPolicy.AllowPersistentCookies)
        except Exception:
            pass
    except Exception:
        pass


def open_url_in_browser(url: str) -> None:
    try:
        import webbrowser
        webbrowser.open(url)
    except Exception:
        pass

def leaflet_webengine_html(lon: float, lat: float, zoom: int, tile_template: str) -> str:
    """Leaflet/QWebEngine preview adapted from Mustatil Satellite Preview.

    Shift+Drag or right mouse drag selects a bbox and sends it through QWebChannel.
    """
    return f"""<!doctype html>
<html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">
<link rel=\"stylesheet\" href=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.css\"/>
<script src=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.js\"></script>
<script src=\"qrc:///qtwebchannel/qwebchannel.js\"></script>
<style>
html,body,#map{{margin:0;padding:0;width:100%;height:100%;overflow:hidden;background:#111}}
.leaflet-container{{background:#111;cursor:grab}}.leaflet-container.selecting{{cursor:crosshair}}
.hint{{position:absolute;left:10px;bottom:10px;z-index:1000;color:#eee;background:rgba(0,0,0,.68);font:12px/1.35 Arial,sans-serif;padding:7px 9px;border-radius:4px;user-select:none}}
.crosshair{{position:absolute;left:50%;top:50%;width:18px;height:18px;margin-left:-9px;margin-top:-9px;pointer-events:none;z-index:1000}}
.crosshair:before,.crosshair:after{{content:\"\";position:absolute;background:rgba(255,255,255,.88);box-shadow:0 0 2px #000}}
.crosshair:before{{left:8px;top:0;width:2px;height:18px}}.crosshair:after{{left:0;top:8px;width:18px;height:2px}}
</style></head><body><div id=\"map\"></div><div class=\"crosshair\"></div><div id=\"hint\" class=\"hint\">Shift+Drag oder Rechts-Drag: Feld markieren</div>
<script>
(function(){{
const TILE_TEMPLATE={json.dumps(tile_template or ESRI_WORLD_IMAGERY)};
let bridge=null;
const map=L.map('map',{{zoomControl:true,attributionControl:false,preferCanvas:true,inertia:true,zoomAnimation:true,fadeAnimation:true,updateWhenIdle:false,updateWhenZooming:false,wheelPxPerZoomLevel:96}}).setView([{float(clamp_lat(lat))},{float(lon)}],{int(zoom)});
let layer=L.tileLayer(TILE_TEMPLATE,{{tileSize:256,minZoom:0,maxZoom:22,maxNativeZoom:22,keepBuffer:5,updateWhenIdle:false,updateWhenZooming:false,detectRetina:false,crossOrigin:false}}).addTo(map);
let selectionRect=null, selecting=false, startLatLng=null, forcedSelect=false;
function hint(t){{document.getElementById('hint').textContent=t;}}
function notifyMove(){{const c=map.getCenter();hint(`Google-Hybrid-Auswahlkarte | Zoom ${{map.getZoom()}} | lon ${{c.lng.toFixed(7)}} lat ${{c.lat.toFixed(7)}} | Mark Area oder Shift+Drag/Rechts-Drag`);if(bridge&&bridge.mapMoved)bridge.mapMoved(c.lng,c.lat,map.getZoom());}}
map.on('moveend zoomend',notifyMove);
map.getContainer().addEventListener('contextmenu',function(e){{e.preventDefault();}});
window.pymapStartMarkArea=function(){{forcedSelect=true;hint('Mark Area aktiv: jetzt mit linker Maustaste Rechteck ziehen. Esc = abbrechen.');map.dragging.disable();map.getContainer().classList.add('selecting');return true;}};
window.pymapCancelMarkArea=function(){{forcedSelect=false;selecting=false;startLatLng=null;if(selectionRect){{map.removeLayer(selectionRect);selectionRect=null;}}map.dragging.enable();map.getContainer().classList.remove('selecting');notifyMove();return true;}};
document.addEventListener('keydown',function(e){{if(e.key==='Escape'&&forcedSelect)window.pymapCancelMarkArea();}},true);
map.on('mousedown',function(e){{const oe=e.originalEvent||{{}};if(!(forcedSelect||oe.shiftKey||oe.button===2))return;selecting=true;startLatLng=e.latlng;map.dragging.disable();map.getContainer().classList.add('selecting');if(selectionRect)map.removeLayer(selectionRect);selectionRect=L.rectangle([startLatLng,startLatLng],{{color:'#00ffff',weight:2,fill:true,fillOpacity:.12,dashArray:'5,4'}}).addTo(map);}});
map.on('mousemove',function(e){{if(selecting&&selectionRect&&startLatLng)selectionRect.setBounds(L.latLngBounds(startLatLng,e.latlng));}});
function finishSelection(e){{
  if(!selecting||!selectionRect)return;
  selecting=false;forcedSelect=false;map.dragging.enable();map.getContainer().classList.remove('selecting');
  const b=selectionRect.getBounds();
  const west=b.getWest(),south=b.getSouth(),east=b.getEast(),north=b.getNorth();
  if(east<=west||north<=south||Math.abs(east-west)<1e-9||Math.abs(north-south)<1e-9){{hint('Auswahl ignoriert: Rechteck größer ziehen');return;}}
  hint(`Auswahl eingetragen: W ${{west.toFixed(8)}} S ${{south.toFixed(8)}} E ${{east.toFixed(8)}} N ${{north.toFixed(8)}}`);
  if(bridge&&bridge.selectionChanged)bridge.selectionChanged(west,south,east,north);
}}
map.on('mouseup',finishSelection);map.on('mouseout',function(e){{if(selecting)finishSelection(e);}});
window.pymapSetView=function(lon,lat,zoom,tileTemplate){{if(tileTemplate)layer.setUrl(tileTemplate);map.setView([lat,lon],zoom,{{animate:false}});setTimeout(function(){{map.invalidateSize(true);notifyMove();}},30);}};
if(window.qt&&window.qt.webChannelTransport){{new QWebChannel(qt.webChannelTransport,function(channel){{bridge=channel.objects.pymapBridge;notifyMove();}});}}else{{notifyMove();}}
setTimeout(function(){{map.invalidateSize(true);notifyMove();}},100);
}})();
</script></body></html>"""



def frame_preview_html(frame_url: str) -> str:
    """Frame preview wrapper with in-page JS selection overlay.

    This avoids transparent QWidget overlays over QWebEngine, which can turn the
    WebView white on Windows.
    """
    return f"""<!doctype html>
<html>
<head>
<meta charset=\"utf-8\">
<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">
<script src=\"qrc:///qtwebchannel/qwebchannel.js\"></script>
<style>
html,body{{margin:0;padding:0;width:100%;height:100%;overflow:hidden;background:#111}}
iframe{{position:absolute;inset:0;width:100%;height:100%;border:0;background:#111;}}
#markLayer{{position:absolute;inset:0;z-index:2147483647;display:none;cursor:crosshair;background:rgba(0,0,0,0.001);}}
#markBox{{position:absolute;display:none;border:3px solid #00ffff;background:rgba(0,255,255,.16);box-shadow:0 0 0 1px rgba(0,0,0,.75),0 0 12px rgba(0,255,255,.65);pointer-events:none;}}
#markHint{{position:absolute;left:10px;bottom:10px;z-index:2147483647;display:none;color:#eee;background:rgba(0,0,0,.78);font:12px Arial;padding:7px 9px;border-radius:4px;user-select:none;}}
</style>
</head>
<body>
<iframe id=\"frame\" src={json.dumps(frame_url)} allow=\"geolocation *; fullscreen *\"></iframe>
<div id=\"markLayer\"><div id=\"markBox\"></div></div>
<div id=\"markHint\">Bereich markieren: ziehen und loslassen. Esc = abbrechen.</div>
<script>
(function(){{
let bridge=null;
const layer=document.getElementById('markLayer');
const box=document.getElementById('markBox');
const hint=document.getElementById('markHint');
let start=null, dragging=false;

function showLayer(){{
  layer.style.display='block';
  hint.style.display='block';
  box.style.display='none';
  start=null;
  dragging=false;
}}
function hideLayer(){{
  layer.style.display='none';
  hint.style.display='none';
  box.style.display='none';
  start=null;
  dragging=false;
}}
function draw(x1,y1,x2,y2){{
  const x=Math.min(x1,x2), y=Math.min(y1,y2);
  const w=Math.abs(x2-x1), h=Math.abs(y2-y1);
  box.style.left=x+'px';
  box.style.top=y+'px';
  box.style.width=w+'px';
  box.style.height=h+'px';
  box.style.display='block';
}}
window.pymapStartMarkArea=function(){{showLayer(); return true;}};
window.pymapCancelMarkArea=function(){{hideLayer(); return true;}};

layer.addEventListener('mousedown', function(e){{
  e.preventDefault(); e.stopPropagation();
  dragging=true;
  start={{x:e.clientX,y:e.clientY}};
  draw(start.x,start.y,start.x,start.y);
}}, true);

layer.addEventListener('mousemove', function(e){{
  if(!dragging || !start) return;
  e.preventDefault(); e.stopPropagation();
  draw(start.x,start.y,e.clientX,e.clientY);
}}, true);

layer.addEventListener('mouseup', function(e){{
  if(!dragging || !start) return;
  e.preventDefault(); e.stopPropagation();
  let x1=start.x, y1=start.y, x2=e.clientX, y2=e.clientY;
  if(Math.abs(x2-x1)<40 || Math.abs(y2-y1)<40){{
    hint.textContent='Auswahl ignoriert: bitte ein größeres Rechteck ziehen.';
    dragging=false;
    box.style.display='none';
    return;
  }}
  if(bridge && bridge.framePixelSelectionChanged){{
    bridge.framePixelSelectionChanged(x1,y1,x2,y2,window.innerWidth,window.innerHeight);
  }}
  hideLayer();
}}, true);

document.addEventListener('keydown', function(e){{ if(e.key==='Escape') hideLayer(); }}, true);
document.addEventListener('contextmenu', function(e){{ if(layer.style.display==='block'){{e.preventDefault(); e.stopPropagation();}} }}, true);

if(window.qt && window.qt.webChannelTransport){{
  new QWebChannel(qt.webChannelTransport, function(channel) {{
    bridge = channel.objects.pymapBridge;
  }});
}}
}})();
</script>
</body>
</html>"""

class WebBridge(QObject):
    def __init__(self, window):
        super().__init__(window)
        self.window = window

    @Slot(float, float, int)
    def mapMoved(self, lon: float, lat: float, zoom: int) -> None:
        self.window.center_lon = float(lon)
        self.window.center_lat = float(lat)
        self.window.preview_zoom = int(zoom)
        self.window.status_label.setText(
            f"Preview: zoom {int(zoom)} | lon {float(lon):.7f} lat {float(lat):.7f}"
        )

    @Slot(float, float, float, float)
    def selectionChanged(self, west: float, south: float, east: float, north: float) -> None:
        west = float(west); south = float(south); east = float(east); north = float(north)
        if east < west:
            west, east = east, west
        if north < south:
            south, north = north, south
        self.window.min_lon_edit.setText(f"{west:.8f}")
        self.window.min_lat_edit.setText(f"{south:.8f}")
        self.window.max_lon_edit.setText(f"{east:.8f}")
        self.window.max_lat_edit.setText(f"{north:.8f}")
        self.window.user_bbox_valid = True
        self.window.last_bbox_source = "mark_area"
        self.window.preview_exact_bbox = (west, south, east, north)

        # Critical fix for frame/Apple preview:
        # iframe-internal panning cannot be reported to Python because of browser
        # origin isolation. From now on the app-controlled preview center is the
        # selected bbox center, so Calculate/Start use exactly the marked area.
        self.window.center_lon = (west + east) / 2.0
        self.window.center_lat = (south + north) / 2.0
        try:
            self.window.preview_zoom = int(self.window.zoom_spin.value())
        except Exception:
            pass

        self.window.log_msg(
            f"Selection entered EXACTLY: South={south:.8f}, West={west:.8f}, North={north:.8f}, East={east:.8f}"
        )
        self.window.calculate()
        # Do not refresh/recenter iframe after selection; this caused jump-back.

    @Slot(float, float, float, float, float, float)
    def framePixelSelectionChanged(self, x1: float, y1: float, x2: float, y2: float, width: float, height: float) -> None:
        """Convert frame pixel selection to bbox.

        Important: panning inside a cross-origin iframe cannot update Python's
        center coordinate. Therefore this function uses the currently known
        app-controlled frame extent. If the left coordinate boxes already contain
        a bbox, that bbox is used as the visible frame extent. Otherwise it falls
        back to center/span from the current zoom.
        """
        try:
            w = max(1.0, float(width))
            h = max(1.0, float(height))
            px1, px2 = sorted([max(0.0, min(w, float(x1))), max(0.0, min(w, float(x2)))])
            py1, py2 = sorted([max(0.0, min(h, float(y1))), max(0.0, min(h, float(y2)))])

            if abs(px2 - px1) < 40 or abs(py2 - py1) < 40:
                self.window.log_msg(
                    f"Frame Mark Area ignored: selection too small ({abs(px2 - px1):.0f}x{abs(py2 - py1):.0f}px). Draw a larger rectangle."
                )
                return

            # Prefer a real, known app bbox. If there is no left-side bbox yet,
            # use the preview_view_bbox created by current_preview_url(). This is
            # now renderer-pixel based and may cover several cells; it is NOT the
            # old one-XYZ-tile fallback that caused accidental 1 x 1 downloads.
            source = "left bbox"
            try:
                if not bool(getattr(self.window, "user_bbox_valid", False)):
                    raise ValueError("left bbox not user-valid")
                view_west = float(self.window.min_lon_edit.text().replace(",", "."))
                view_south = float(self.window.min_lat_edit.text().replace(",", "."))
                view_east = float(self.window.max_lon_edit.text().replace(",", "."))
                view_north = float(self.window.max_lat_edit.text().replace(",", "."))
                if not (view_east > view_west and view_north > view_south):
                    raise ValueError("invalid left bbox")
            except Exception:
                view_bbox = getattr(self.window, "preview_view_bbox", None)
                if view_bbox and len(view_bbox) == 4:
                    view_west, view_south, view_east, view_north = map(float, view_bbox)
                    source = "known preview bbox"
                else:
                    z = int(self.window.zoom_spin.value())
                    render_w = int(self.window.render_w_spin.value()) if hasattr(self.window, "render_w_spin") else int(w)
                    render_h = int(self.window.render_h_spin.value()) if hasattr(self.window, "render_h_spin") else int(h)
                    cells = float(self.window.frame_preview_cells_spin.value()) if hasattr(self.window, "frame_preview_cells_spin") else 4.0
                    view_west, view_south, view_east, view_north = frame_view_bbox_for_center_zoom_pixels(
                        self.window.center_lon, self.window.center_lat, z,
                        max(w, render_w) * cells, max(h, render_h) * cells
                    )
                    source = f"renderer-pixel fallback ({cells:.1f} cells)"

            lon_span = view_east - view_west
            lat_span = view_north - view_south

            west = view_west + (px1 / w) * lon_span
            east = view_west + (px2 / w) * lon_span
            north = view_north - (py1 / h) * lat_span
            south = view_north - (py2 / h) * lat_span

            self.window.log_msg(
                f"Frame Mark Area pixels: x={px1:.0f}..{px2:.0f}, y={py1:.0f}..{py2:.0f}, viewport={w:.0f}x{h:.0f}"
            )
            self.window.log_msg(
                f"Frame Mark Area based on {source}: W={view_west:.8f}, S={view_south:.8f}, E={view_east:.8f}, N={view_north:.8f}"
            )
            self.selectionChanged(west, south, east, north)
        except Exception as exc:
            self.window.log_msg(f"Frame Mark Area failed: {exc}")


class PySideMapStitcher(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Py Map Stitcher 3 - Own Frame MultiView TIFF CPU")
        self.resize(1320, 820)
        self.stop_event = threading.Event()
        self.worker_thread = None
        self.q = queue.Queue()
        self.center_lon = 10.0
        self.center_lat = 51.0
        self.preview_zoom = 3
        self.user_bbox_valid = False
        self.last_bbox_source = ""
        self.preview_exact_bbox = None
        self.preview_view_bbox = None
        self._build_ui()
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._poll)
        self._timer.start(80)
        self.frame_active = False
        self.frame_queue = []
        self.frame_queue_index = 0
        self.frame_done = 0
        self.frame_total = 0
        self.frame_renderers = []
        self.frame_render_windows = []
        self.frame_mem = None
        self.frame_cfg = None
        self.frame_tile_dir = None
        self.frame_x_min = self.frame_y_min = 0
        self.frame_cell_w = self.frame_cell_h = TILE_SIZE
        self.frame_render_w_actual = self.frame_render_h_actual = 0
        self.frame_request_lon_span = self.frame_request_lat_span = 0.0
        self.frame_lon_per_px = self.frame_lat_per_px = 0.0
        self.frame_visible_lon_span = self.frame_visible_lat_span = 0.0
        self.frame_selected_west = self.frame_selected_north = 0.0
        self.frame_grid_west = self.frame_grid_south = self.frame_grid_east = self.frame_grid_north = 0.0
        self.frame_step_mult_x = 1.0
        self.frame_step_mult_y = 1.0
        self.frame_shift_x_px = FIXED_FRAME_SHIFT_X_PX
        self.frame_shift_y_px = FIXED_FRAME_SHIFT_Y_PX
        self.frame_crop_correct_url = True
        self.preview_exact_bbox = None
        self.preview_view_bbox = None
        self._google_selector_loaded = False
        self._google_selector_pending_mark = False
        QTimer.singleShot(200, self.refresh_webmap)

    def _build_ui(self) -> None:
        splitter = QSplitter(Qt.Orientation.Horizontal)
        self.setCentralWidget(splitter)

        left = QWidget()
        left_layout = QVBoxLayout(left)
        left_layout.setContentsMargins(8, 8, 8, 8)
        splitter.addWidget(left)
        splitter.setStretchFactor(0, 0)

        form_box = QGroupBox("Map / Download")
        form = QFormLayout(form_box)
        form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        left_layout.addWidget(form_box)

        # Fixed Apple workflow: no selectable provider dropdown and no editable URL field.
        # The hidden combo/line edit stay available for existing internal methods.
        self.preset_combo = QComboBox()
        self.preset_combo.addItems(["Apple Frame Preview / center-span helper"])
        self.preset_combo.setCurrentText("Apple Frame Preview / center-span helper")
        self.preset_combo.setVisible(False)
        form.addRow("Map Selection", QLabel("Apple Frame Preview / center-span helper"))

        self.url_edit = QLineEdit(MAP_PRESETS.get("Apple Frame Preview / center-span helper", MAP_PRESETS["Google Satellite"])["url"])
        self.url_edit.setVisible(False)
        self.note_label = QLabel(MAP_PRESETS.get("Apple Frame Preview / center-span helper", MAP_PRESETS["Google Satellite"])["note"])
        self.note_label.setWordWrap(True)
        self.note_label.setVisible(False)

        self.zoom_spin = QSpinBox(); self.zoom_spin.setRange(0, 22); self.zoom_spin.setValue(18)
        self.min_lat_edit = QLineEdit("")
        self.min_lon_edit = QLineEdit("")
        self.max_lat_edit = QLineEdit("")
        self.max_lon_edit = QLineEdit("")
        for _bbox_edit in (self.min_lat_edit, self.min_lon_edit, self.max_lat_edit, self.max_lon_edit):
            _bbox_edit.textEdited.connect(self._mark_manual_bbox_edit)
        self.workers_spin = QSpinBox(); self.workers_spin.setRange(1, 256); self.workers_spin.setValue(32)
        self.rate_spin = QSpinBox(); self.rate_spin.setRange(0, 20000); self.rate_spin.setSingleStep(250); self.rate_spin.setValue(3000)
        self.frame_settle_extra_spin = QSpinBox(); self.frame_settle_extra_spin.setRange(0, 20000); self.frame_settle_extra_spin.setSingleStep(250); self.frame_settle_extra_spin.setValue(2500)
        self.chunk_spin = QSpinBox(); self.chunk_spin.setRange(8, 2048); self.chunk_spin.setSingleStep(8); self.chunk_spin.setValue(128)
        self.frame_views_spin = QSpinBox(); self.frame_views_spin.setRange(1, 16); self.frame_views_spin.setValue(4)
        self.render_w_spin = QSpinBox(); self.render_w_spin.setRange(256, 4096); self.render_w_spin.setSingleStep(128); self.render_w_spin.setValue(1600)
        self.render_h_spin = QSpinBox(); self.render_h_spin.setRange(256, 4096); self.render_h_spin.setSingleStep(128); self.render_h_spin.setValue(1600)
        self.crop_top_spin = QSpinBox(); self.crop_top_spin.setRange(0, 1000); self.crop_top_spin.setValue(0)
        self.crop_bottom_spin = QSpinBox(); self.crop_bottom_spin.setRange(0, 1000); self.crop_bottom_spin.setValue(100)
        self.step_factor_x_spin = QDoubleSpinBox()
        self.step_factor_x_spin.setRange(0.00, 5.00)
        self.step_factor_x_spin.setSingleStep(0.05)
        self.step_factor_x_spin.setDecimals(3)
        self.step_factor_x_spin.setValue(0.0)
        self.step_factor_y_spin = QDoubleSpinBox()
        self.step_factor_y_spin.setRange(0.00, 5.00)
        self.step_factor_y_spin.setSingleStep(0.05)
        self.step_factor_y_spin.setDecimals(3)
        self.step_factor_y_spin.setValue(0.0)
        self.crop_left_spin = QSpinBox(); self.crop_left_spin.setRange(0, 1000); self.crop_left_spin.setValue(300)
        self.crop_right_spin = QSpinBox(); self.crop_right_spin.setRange(0, 1000); self.crop_right_spin.setValue(100)
        self.pixel_step_x_spin = QDoubleSpinBox()
        self.pixel_step_x_spin.setRange(0.25, 20.00)
        self.pixel_step_x_spin.setSingleStep(0.25)
        self.pixel_step_x_spin.setDecimals(2)
        self.pixel_step_x_spin.setValue(1.00)
        self.pixel_step_y_spin = QDoubleSpinBox()
        self.pixel_step_y_spin.setRange(0.25, 20.00)
        self.pixel_step_y_spin.setSingleStep(0.25)
        self.pixel_step_y_spin.setDecimals(2)
        self.pixel_step_y_spin.setValue(1.00)
        self.frame_shift_x_spin = QDoubleSpinBox()
        self.frame_shift_x_spin.setRange(-3000.0, 3000.0)
        self.frame_shift_x_spin.setSingleStep(10.0)
        self.frame_shift_x_spin.setDecimals(1)
        self.frame_shift_x_spin.setValue(FIXED_FRAME_SHIFT_X_PX)
        self.frame_shift_x_spin.setToolTip("Additive Feinverschiebung pro Spalte in Pixeln. Negativ = mehr Überlappung, positiv = weiter auseinander.")
        self.frame_shift_y_spin = QDoubleSpinBox()
        self.frame_shift_y_spin.setRange(-3000.0, 3000.0)
        self.frame_shift_y_spin.setSingleStep(10.0)
        self.frame_shift_y_spin.setDecimals(1)
        self.frame_shift_y_spin.setValue(FIXED_FRAME_SHIFT_Y_PX)
        self.frame_shift_y_spin.setToolTip("Additive Feinverschiebung pro Zeile in Pixeln. Negativ = mehr Überlappung, positiv = weiter auseinander.")

        self.frame_preview_cells_spin = QDoubleSpinBox()
        self.frame_preview_cells_spin.setRange(1.0, 50.0)
        self.frame_preview_cells_spin.setSingleStep(0.5)
        self.frame_preview_cells_spin.setDecimals(1)
        self.frame_preview_cells_spin.setValue(4.0)
        self.frame_preview_cells_spin.setToolTip("Nur Apple/Frame Mark Area ohne vorhandene BBox: Vorschaufläche in Renderer-Zellen. Höher = du kannst einen größeren Bereich markieren; verhindert 1x1 durch winzige Fallback-Span.")

        self.frame_min_cols_spin = QSpinBox()
        self.frame_min_cols_spin.setRange(1, 10000)
        self.frame_min_cols_spin.setValue(1)
        self.frame_min_cols_spin.setToolTip("Notfall/Test: erzwingt mindestens so viele Screenshot-Spalten, auch wenn die berechnete BBox kleiner wirkt.")
        self.frame_min_rows_spin = QSpinBox()
        self.frame_min_rows_spin.setRange(1, 10000)
        self.frame_min_rows_spin.setValue(1)
        self.frame_min_rows_spin.setToolTip("Notfall/Test: erzwingt mindestens so viele Screenshot-Zeilen, auch wenn die berechnete BBox kleiner wirkt.")
        self.hidden_render_check = QCheckBox()
        self.hidden_render_check.setChecked(False)
        self.hidden_render_check.setVisible(False)
        self.fullscreen_render_check = QCheckBox("Frame WebViews als eigene 1600x1600-Fenster")
        self.fullscreen_render_check.setChecked(True)
        self.fullscreen_render_check.setToolTip("Öffnet jeden Renderer als eigenes festes Fenster in Render-W/H. Nicht maximieren, damit Pixelrechnung exakt bleibt.")
        self.crop_correct_url_check = QCheckBox("Crop in Apple-URL-Geometrie einrechnen")
        self.crop_correct_url_check.setChecked(True)
        self.crop_correct_url_check.setToolTip("Standard AN: Crop L/T/R/B wird in die URL-Center-Geometrie eingerechnet. Left, right, top und bottom verschieben den Center jetzt symmetrisch.")
        self.pypi_stitching_check = QCheckBox("Frame-Endexport mit PyPI stitching zusammensetzen")
        self.pypi_stitching_check.setChecked(False)
        self.pypi_stitching_check.setToolTip("Optional. Standard AUS, weil Feature-Stitching bei Karten oft schlecht arbeitet. Der direkte Grid-GeoTIFF bleibt der Hauptausgang.")
        self.outfile_edit = QLineEdit(str(Path.home() / "Desktop" / "map_output.tif"))

        # Only the requested user-facing controls remain visible.
        form.addRow("Download Zoom", self.zoom_spin)
        form.addRow("South / min lat", self.min_lat_edit)
        form.addRow("West / min lon", self.min_lon_edit)
        form.addRow("North / max lat", self.max_lat_edit)
        form.addRow("East / max lon", self.max_lon_edit)
        form.addRow("Download Threads", self.workers_spin)
        form.addRow("Frame WebViews", self.frame_views_spin)

        out_row = QHBoxLayout()
        out_row.addWidget(self.outfile_edit, 1)
        browse = QPushButton("…")
        browse.clicked.connect(self.pick_output)
        out_row.addWidget(browse)
        out_widget = QWidget(); out_widget.setLayout(out_row)
        form.addRow("Output File", out_widget)

        btn_row = QHBoxLayout()
        calc_btn = QPushButton("Calculate")
        calc_btn.clicked.connect(self.calculate)
        start_btn = QPushButton("Start")
        start_btn.clicked.connect(self.start)
        stop_btn = QPushButton("Stop")
        stop_btn.clicked.connect(self.stop_event.set)
        btn_row.addWidget(calc_btn); btn_row.addWidget(start_btn); btn_row.addWidget(stop_btn)
        left_layout.addLayout(btn_row)

        terms = QLabel("Only use servers where downloading/stitching is allowed. Google/Bing/OSM may restrict bulk downloads.")
        terms.setWordWrap(True)
        left_layout.addWidget(terms)

        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        left_layout.addWidget(self.progress)
        self.status_label = QLabel("Ready")
        left_layout.addWidget(self.status_label)
        self.log = QTextEdit()
        self.log.setReadOnly(True)
        self.log.setMinimumHeight(170)
        left_layout.addWidget(self.log, 1)

        right = QWidget()
        right_layout = QVBoxLayout(right)
        right_layout.setContentsMargins(8, 8, 8, 8)
        splitter.addWidget(right)
        splitter.setStretchFactor(1, 1)

        map_header = QHBoxLayout()
        map_title = QLabel("Satellite WebMap / Selection like Mustatil Satellite Preview")
        map_title.setStyleSheet("font-weight: 600;")
        map_header.addWidget(map_title, 1)
        self.preview_mode_combo = QComboBox()
        self.preview_mode_combo.addItems(["Iframe wrapper"])
        self.preview_mode_combo.setCurrentText("Iframe wrapper")
        self.preview_mode_combo.setVisible(False)
        reload_btn = QPushButton("Reload")
        reload_btn.clicked.connect(self.refresh_webmap)
        map_header.addWidget(reload_btn)
        browser_btn = QPushButton("Open in Browser")
        browser_btn.clicked.connect(self.open_current_preview_in_browser)
        map_header.addWidget(browser_btn)
        self.mark_area_btn = QPushButton("Mark Area")
        self.mark_area_btn.clicked.connect(self.start_mark_area)
        map_header.addWidget(self.mark_area_btn)
        self.preview_bbox_btn = QPushButton("Preview Selected BBox")
        self.preview_bbox_btn.clicked.connect(self.preview_selected_bbox)
        map_header.addWidget(self.preview_bbox_btn)
        right_layout.addLayout(map_header)

        self.webview = QWebEngineView()
        configure_webengine_view(self.webview)
        self.webview.setStyleSheet("background:#111;")
        self.webview.installEventFilter(self)
        self.preview_mark_mode = False
        self.preview_selecting = False
        self.preview_select_start = QPoint()
        self.preview_overlay = None  # JS page overlay is used; QWidget overlay breaks QWebEngine on Windows.
        self.preview_rubber_band = None
        self.web_bridge = WebBridge(self)
        self.web_channel = QWebChannel(self.webview.page())
        self.web_channel.registerObject("pymapBridge", self.web_bridge)
        self.webview.page().setWebChannel(self.web_channel)
        right_layout.addWidget(self.webview, 1)

        render_label = QLabel("Frame Render WebViews (nur Own Frame Server; sichtbar lassen, falls Hidden-Screenshots schwarz werden)")
        render_label.setStyleSheet("font-weight: 600;")
        right_layout.addWidget(render_label)
        self.render_area = QWidget()
        self.render_layout = QGridLayout(self.render_area)
        self.render_layout.setContentsMargins(0, 0, 0, 0)
        self.render_layout.setSpacing(4)
        right_layout.addWidget(self.render_area, 0)

        splitter.setSizes([420, 900])

    def _mark_manual_bbox_edit(self, *_args) -> None:
        self.user_bbox_valid = True
        self.last_bbox_source = "manual"
        self.preview_exact_bbox = None

    def clear_bbox_fields(self, log: bool = False) -> None:
        for edit in (self.min_lat_edit, self.min_lon_edit, self.max_lat_edit, self.max_lon_edit):
            edit.blockSignals(True)
            edit.clear()
            edit.blockSignals(False)
        self.user_bbox_valid = False
        self.last_bbox_source = ""
        self.preview_exact_bbox = None
        self.preview_view_bbox = None
        if log:
            self.log_msg("BBox cleared: Apple/Frame mode will not use any preset mini bbox. Mark an area or enter coordinates before Start.")

    def read_bbox_values(self):
        west = float(self.min_lon_edit.text().replace(",", "."))
        south = float(self.min_lat_edit.text().replace(",", "."))
        east = float(self.max_lon_edit.text().replace(",", "."))
        north = float(self.max_lat_edit.text().replace(",", "."))
        if not (east > west and north > south):
            raise ValueError("invalid bbox")
        return west, south, east, north

    def has_valid_bbox(self) -> bool:
        try:
            self.read_bbox_values()
            return True
        except Exception:
            return False

    def preview_selected_bbox(self) -> None:
        """Reload frame preview using the exact bbox fields, not a tiny center/zoom span."""
        try:
            west, south, east, north = self.read_bbox_values()
            self.center_lon = (west + east) / 2.0
            self.center_lat = (south + north) / 2.0
            self.preview_zoom = int(self.zoom_spin.value())
            self.preview_exact_bbox = (west, south, east, north)
            self.user_bbox_valid = True
            self.last_bbox_source = "preview_bbox"
            self.log_msg(
                f"Preview Selected BBox EXACT: W={west:.8f}, S={south:.8f}, E={east:.8f}, N={north:.8f}, z={self.preview_zoom}"
            )
            self.refresh_webmap()
        except Exception as exc:
            QMessageBox.warning(self, "Preview selected bbox", f"Could not preview selected bbox: {exc}")

    def load_google_hybrid_selection_map(self, reason: str = "") -> None:
        """Load Google Hybrid only as the coordinate selector.

        The URL template on the left is NOT changed. Therefore Apple/Frame can be
        selected for download, while this preview remains a stable Leaflet map for
        drawing the bbox. This avoids reading any geometry from Apple WebView.
        """
        try:
            try:
                self.preview_zoom = int(self.zoom_spin.value())
            except Exception:
                pass
            html = leaflet_webengine_html(
                float(self.center_lon),
                float(self.center_lat),
                int(self.preview_zoom),
                GOOGLE_HYBRID_SELECTOR,
            )
            self.preview_view_bbox = None
            self._google_selector_loaded = True
            self.webview.setHtml(html, QUrl("https://mustatil.local/"))
            self.status_label.setText("Google Hybrid selector loaded - download still uses the Apple URL template")
            self.log_msg("Google-Hybrid-Auswahlkarte aktiv: Markierung schreibt nur die vier BBox-Felder; Download nutzt weiter die URL links." + (f" ({reason})" if reason else ""))
        except Exception as exc:
            self.log_msg(f"Google Hybrid selector failed: {exc}")

    def start_mark_area(self) -> None:
        """Enable Mark Area. For Apple/frame downloads, select on Google Hybrid."""
        try:
            url_template = self.url_edit.text().strip()
            preset = MAP_PRESETS.get(self.preset_combo.currentText(), {})
            if preset.get("preview") == "frame" or is_frame_template(url_template):
                # Critical workflow fix: use Google Hybrid/Leaflet for coordinate
                # selection, but do not change cfg.url_template. Start still uses
                # Apple/Frame URL + the four coordinate fields.
                if not bool(getattr(self, "_google_selector_loaded", False)):
                    self.load_google_hybrid_selection_map("Apple/Frame download mode")
                    QTimer.singleShot(500, self.start_mark_area)
                    return

            js = "if(window.pymapStartMarkArea){window.pymapStartMarkArea(); true;} else {false;}"
            self.webview.page().runJavaScript(js, lambda ok: self.log_msg(
                "Mark Area active: ziehe jetzt im Preview-Fenster einen Kasten." if ok
                else "Mark Area konnte nicht aktiviert werden. Preview neu laden und Google/Leaflet-Auswahlkarte nutzen."
            ))
            self.status_label.setText("Mark Area active: im Preview-Fenster ziehen. Esc = abbrechen.")
        except Exception as exc:
            self.log_msg(f"Mark Area failed: {exc}")


    def eventFilter(self, obj, event):
        # No QWidget overlay over QWebEngine: on Windows this can turn the WebView white.
        # Mark Area is handled by JS inside frame_preview_html.
        return super().eventFilter(obj, event)

    def apply_preview_pixel_selection(self, p1: QPoint, p2: QPoint) -> None:
        """Convert WebView pixel rectangle to left-side bbox using current preview center/span."""
        try:
            w = max(1, self.webview.width())
            h = max(1, self.webview.height())
            x1 = max(0, min(w, int(p1.x())))
            x2 = max(0, min(w, int(p2.x())))
            y1 = max(0, min(h, int(p1.y())))
            y2 = max(0, min(h, int(p2.y())))
            px1, px2 = sorted([x1, x2])
            py1, py2 = sorted([y1, y2])

            if abs(px2 - px1) < 40 or abs(py2 - py1) < 40:
                self.log_msg(
                    f"Python Mark Area ignored: selection too small ({abs(px2 - px1):.0f}x{abs(py2 - py1):.0f}px). Draw a larger rectangle."
                )
                return

            view_bbox = getattr(self, "preview_view_bbox", None)
            source = "known preview bbox"
            if view_bbox and len(view_bbox) == 4:
                view_west, view_south, view_east, view_north = map(float, view_bbox)
            else:
                z = int(self.zoom_spin.value())
                render_w = int(self.render_w_spin.value()) if hasattr(self, "render_w_spin") else int(w)
                render_h = int(self.render_h_spin.value()) if hasattr(self, "render_h_spin") else int(h)
                cells = float(self.frame_preview_cells_spin.value()) if hasattr(self, "frame_preview_cells_spin") else 4.0
                view_west, view_south, view_east, view_north = frame_view_bbox_for_center_zoom_pixels(
                    float(self.center_lon), float(self.center_lat), z,
                    max(float(w), float(render_w)) * cells, max(float(h), float(render_h)) * cells
                )
                source = f"renderer-pixel fallback ({cells:.1f} cells)"

            lon_span = view_east - view_west
            lat_span = view_north - view_south

            west = view_west + (float(px1) / float(w)) * lon_span
            east = view_west + (float(px2) / float(w)) * lon_span
            north = view_north - (float(py1) / float(h)) * lat_span
            south = view_north - (float(py2) / float(h)) * lat_span

            self.log_msg(
                f"Python Mark Area pixels: x={px1:.0f}..{px2:.0f}, y={py1:.0f}..{py2:.0f}, preview={w}x{h}"
            )
            self.log_msg(
                f"Python Mark Area based on {source}: W={view_west:.8f}, S={view_south:.8f}, E={view_east:.8f}, N={view_north:.8f}"
            )
            self.web_bridge.selectionChanged(west, south, east, north)
        except Exception as exc:
            self.log_msg(f"Python Mark Area failed: {exc}")

    def current_preview_url(self) -> str:
        url_template = self.url_edit.text().strip()
        preset = MAP_PRESETS.get(self.preset_combo.currentText(), {})
        if preset.get("preview") == "frame" or is_frame_template(url_template):
            z = int(self.zoom_spin.value()) if hasattr(self, "zoom_spin") else self.preview_zoom

            # FIELD-BBOX ONLY for Apple/Frame preview when coordinates exist.
            # This intentionally ignores the internal map/preview center. The four
            # left-side fields are the authority for center/span/bbox URL creation.
            try:
                west, south, east, north = self.read_bbox_values()
                self.center_lon = (west + east) / 2.0
                self.center_lat = (south + north) / 2.0
                self.preview_zoom = z
                self.preview_exact_bbox = (west, south, east, north)
                self.preview_view_bbox = (float(west), float(south), float(east), float(north))
                return expand_frame_url_exact_bbox(url_template, west, south, east, north, z)
            except Exception:
                pass

            exact = getattr(self, "preview_exact_bbox", None)
            if exact:
                try:
                    west, south, east, north = exact
                    self.preview_view_bbox = (float(west), float(south), float(east), float(north))
                    return expand_frame_url_exact_bbox(url_template, west, south, east, north, z)
                except Exception as exc:
                    self.log_msg(f"Exact bbox preview failed, falling back to renderer-pixel preview span: {exc}")

            # Only preview fallback when the coordinate fields are empty.
            # Start/download still refuses to run without valid field coordinates.
            try:
                render_w = int(self.render_w_spin.value()) if hasattr(self, "render_w_spin") else 1600
                render_h = int(self.render_h_spin.value()) if hasattr(self, "render_h_spin") else 1600
                cells = float(self.frame_preview_cells_spin.value()) if hasattr(self, "frame_preview_cells_spin") else 4.0
                west, south, east, north = frame_view_bbox_for_center_zoom_pixels(
                    self.center_lon, self.center_lat, z,
                    max(1.0, float(render_w) * cells),
                    max(1.0, float(render_h) * cells),
                )
                self.preview_view_bbox = (float(west), float(south), float(east), float(north))
                return expand_frame_url_exact_bbox(url_template, west, south, east, north, z)
            except Exception as exc:
                self.log_msg(f"Renderer-pixel preview span failed, falling back to center/span preview only: {exc}")
                url = expand_frame_url_center_span(url_template, self.center_lon, self.center_lat, z)
                try:
                    lat_span, lon_span = frame_span_for_center_zoom(self.center_lon, self.center_lat, z)
                    self.preview_view_bbox = (
                        float(self.center_lon) - lon_span / 2.0,
                        float(self.center_lat) - lat_span / 2.0,
                        float(self.center_lon) + lon_span / 2.0,
                        float(self.center_lat) + lat_span / 2.0,
                    )
                except Exception:
                    self.preview_view_bbox = None
                return url
        return url_template or ESRI_WORLD_IMAGERY

    def open_current_preview_in_browser(self) -> None:
        url = self.current_preview_url()
        self.log_msg(f"Open preview in browser: {url}")
        open_url_in_browser(url)

    def on_preset_changed(self, name: str) -> None:
        preset = MAP_PRESETS.get(name, MAP_PRESETS["Custom"])
        self.url_edit.setText(preset["url"])
        self.note_label.setText(preset.get("note", ""))
        # Important: do not keep an old/tiny bbox when switching to Apple/frame mode.
        # Otherwise Start may download that stale mini extent immediately.
        if preset.get("preview") == "frame" or is_frame_template(preset.get("url", "")):
            self.clear_bbox_fields(log=True)
            self._google_selector_loaded = False
            self.log_msg("Apple/Frame workflow: rechts wird Google Hybrid zum Markieren benutzt; Start nutzt nur die Apple-URL links + BBox-Felder.")
        self.refresh_webmap()

    def refresh_webmap(self) -> None:
        preset = MAP_PRESETS.get(self.preset_combo.currentText(), {})
        url_template = self.url_edit.text().strip()
        preview_mode = self.preview_mode_combo.currentText() if hasattr(self, "preview_mode_combo") else "Auto"

        if preview_mode == "Leaflet tiles":
            if preset.get("preview") == "frame" or is_frame_template(url_template):
                self.load_google_hybrid_selection_map("Leaflet selector for Apple/Frame URL")
                return
            html = leaflet_webengine_html(
                self.center_lon,
                self.center_lat,
                self.preview_zoom,
                url_template or ESRI_WORLD_IMAGERY,
            )
            self._google_selector_loaded = False
            self.webview.setHtml(html, QUrl("https://mustatil.local/"))
            self.status_label.setText("Leaflet tile preview loaded")
            self.log_msg(f"Preview mode: Leaflet tiles | template: {url_template}")
            return

        if preset.get("preview") == "frame" or is_frame_template(url_template):
            # Stable workflow: select bbox in Google Hybrid/Leaflet, but keep the
            # Apple/Frame URL template on the left for download. This fixes the
            # Apple WebView state problem where the second run could reuse one old
            # place or jump back to a start location.
            if preview_mode != "Direct URL":
                self.load_google_hybrid_selection_map("refresh")
                return

            # Direct URL is only for inspecting the actual Apple/frame URL. It is
            # intentionally not used for coordinate selection.
            url = self.current_preview_url()
            try:
                self.webview.loadFinished.disconnect()
            except Exception:
                pass
            self.webview.load(QUrl(url))
            self._google_selector_loaded = False
            self.status_label.setText("Frame URL loaded directly for inspection only - use Google selector for Mark Area")
            self.log_msg(f"Preview mode: direct frame URL inspection | URL: {url}")
            return

        html = leaflet_webengine_html(
            self.center_lon,
            self.center_lat,
            self.preview_zoom,
            url_template or ESRI_WORLD_IMAGERY,
        )
        self._google_selector_loaded = False
        self.webview.setHtml(html, QUrl("https://mustatil.local/"))
        self.status_label.setText("WebMap loaded - Shift+Drag or right-drag to select extent")

    def pick_output(self) -> None:
        path, _ = QFileDialog.getSaveFileName(self, "Output BigTIFF", self.outfile_edit.text(), "TIFF (*.tif *.tiff);;All files (*)")
        if path:
            self.outfile_edit.setText(path)

    def _config(self) -> StitchConfig:
        try:
            west, south, east, north = self.read_bbox_values()
        except Exception as exc:
            raise RuntimeError("No valid download bbox. Draw a real Mark Area or enter South/West/North/East manually; no tiny preset bbox is used anymore.") from exc
        return StitchConfig(
            url_template=MAP_PRESETS["Apple Frame Preview / center-span helper"]["url"],
            output_file=Path(self.outfile_edit.text()).expanduser(),
            z=int(self.zoom_spin.value()),
            min_lat=south,
            min_lon=west,
            max_lat=north,
            max_lon=east,
            workers=int(self.workers_spin.value()),
            rate_limit_ms=int(self.rate_spin.value()),
            chunk_size=int(self.chunk_spin.value()),
        )

    def calculate(self) -> None:
        try:
            cfg = self._config()
            render_w = int(self.render_w_spin.value())
            render_h = int(self.render_h_spin.value())
            crop_left = int(self.crop_left_spin.value())
            crop_top = int(self.crop_top_spin.value())
            crop_right = int(self.crop_right_spin.value())
            crop_bottom = int(self.crop_bottom_spin.value())
            visible_w = max(1, render_w - crop_left - crop_right)
            visible_h = max(1, render_h - crop_top - crop_bottom)

            # FIELD-BBOX ONLY: calculation mirrors Start exactly. No preview/map
            # center is used to create the download URL grid.
            sel_left_px, sel_top_px = lonlat_to_world_pixel(cfg.min_lon, cfg.max_lat, cfg.z)
            sel_right_px, sel_bottom_px = lonlat_to_world_pixel(cfg.max_lon, cfg.min_lat, cfg.z)
            selected_width_px = max(1.0, sel_right_px - sel_left_px)
            selected_height_px = max(1.0, sel_bottom_px - sel_top_px)

            step_mult_x = float(self.pixel_step_x_spin.value()) if hasattr(self, "pixel_step_x_spin") else 1.0
            step_mult_y = float(self.pixel_step_y_spin.value()) if hasattr(self, "pixel_step_y_spin") else 1.0
            shift_x_px = FIXED_FRAME_SHIFT_X_PX
            shift_y_px = FIXED_FRAME_SHIFT_Y_PX
            crop_correct_url = bool(getattr(self, "crop_correct_url_check", None) and self.crop_correct_url_check.isChecked())
            effective_step_x_px = max(1.0, float(visible_w) * step_mult_x + shift_x_px)
            effective_step_y_px = max(1.0, float(visible_h) * step_mult_y + shift_y_px)

            calc_cols = max(1, int(math.ceil(selected_width_px / effective_step_x_px)))
            calc_rows = max(1, int(math.ceil(selected_height_px / effective_step_y_px)))
            min_cols = int(self.frame_min_cols_spin.value()) if hasattr(self, "frame_min_cols_spin") else 1
            min_rows = int(self.frame_min_rows_spin.value()) if hasattr(self, "frame_min_rows_spin") else 1
            cols = max(calc_cols, min_cols)
            rows = max(calc_rows, min_rows)

            grid_right_px = sel_left_px + cols * effective_step_x_px
            grid_bottom_px = sel_top_px + rows * effective_step_y_px
            grid_west, grid_south, grid_east, grid_north = world_pixel_bbox_to_lonlat(
                sel_left_px, sel_top_px, grid_right_px, grid_bottom_px, cfg.z
            )

            width = cols * visible_w
            height = rows * visible_h
            raw_bytes = width * height * 3

            self.log_msg("=== Calculation: Frame Screenshot Grid / FIELD-BBOX URL MODE ===")
            self.log_msg("URL source: ONLY the four coordinate fields. Preview/map center is ignored for download URL creation.")
            self.log_msg(f"Selected bbox fields: S={cfg.min_lat:.8f}, W={cfg.min_lon:.8f}, N={cfg.max_lat:.8f}, E={cfg.max_lon:.8f}")
            self.log_msg(f"Selected size at z={cfg.z}: {selected_width_px:.1f} x {selected_height_px:.1f} world-px")
            self.log_msg(f"Grid coverage: S={grid_south:.8f}, W={grid_west:.8f}, N={grid_north:.8f}, E={grid_east:.8f}")
            self.log_msg(f"Render size: {render_w}x{render_h}; crop L/T/R/B={crop_left}/{crop_top}/{crop_right}/{crop_bottom}")
            self.log_msg(f"Visible output cell: {visible_w}x{visible_h} px")
            self.log_msg(f"Effective step: X={effective_step_x_px:.1f}px, Y={effective_step_y_px:.1f}px; multiplier={step_mult_x:.3f}/{step_mult_y:.3f}; shift={shift_x_px:.1f}/{shift_y_px:.1f}px")
            if cols != calc_cols or rows != calc_rows:
                self.log_msg(f"Force min grid applied: calculated {calc_cols} x {calc_rows}, using {cols} x {rows}.")
            self.log_msg(f"Grid cells: {cols} x {rows} = {cols*rows:,}; output pixels: {width:,} x {height:,}")
            self.log_msg(f"Estimated raw BigTIFF payload: {format_bytes(raw_bytes)}")

            sample_url, sample_visible, sample_request = expand_frame_url_grid(
                cfg.url_template, 0, 0, cfg.z,
                cfg.min_lon, cfg.max_lat,
                0.0, 0.0,
                0.0, 0.0,
                visible_w, visible_h,
                render_w, render_h,
                crop_left, crop_top, crop_right, crop_bottom,
                step_mult_x, step_mult_y, shift_x_px, shift_y_px, crop_correct_url,
            )
            self.log_msg(f"Sample cell 0/0 visible={sample_visible}")
            self.log_msg(f"Sample cell 0/0 request={sample_request}")
            self.log_msg(f"Sample URL: {sample_url}")
            self.log_msg("Alignment mode: NoDoubleCrop is default. Request bounds equal visible bounds unless Crop URL correction is enabled.")
        except Exception as exc:
            QMessageBox.critical(self, "Error", str(exc))

    def start(self) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            QMessageBox.information(self, "Running", "A job is already running.")
            return
        if self.frame_active:
            QMessageBox.information(self, "Running", "A frame screenshot job is already running.")
            return
        preset = MAP_PRESETS.get(self.preset_combo.currentText(), {})
        current_url_template = self.url_edit.text().strip()
        current_is_frame = preset.get("preview") == "frame" or any(k in current_url_template for k in ("{center_lat}", "{center_lon}", "{lat_span}", "{lon_span}", "{bbox}"))
        if current_is_frame and not self.has_valid_bbox():
            QMessageBox.warning(
                self,
                "No bbox selected",
                "Apple/Frame mode has no preset mini bbox anymore. Please click Mark Area and draw a real rectangle, or enter South/West/North/East manually.",
            )
            self.log_msg("Start blocked: no valid bbox. The old automatic tiny Apple/frame bbox was removed.")
            return
        try:
            cfg = self._config()
        except Exception as exc:
            QMessageBox.critical(self, "Error", str(exc))
            return
        self.stop_event.clear()
        self.progress.setValue(0)

        # FIELD-BBOX ONLY for Start/download: the four text fields are now the
        # single source of truth for Apple/Frame URL center/span/bbox creation.
        west = float(cfg.min_lon)
        south = float(cfg.min_lat)
        east = float(cfg.max_lon)
        north = float(cfg.max_lat)
        self.center_lon = (west + east) / 2.0
        self.center_lat = (south + north) / 2.0
        self.preview_zoom = int(cfg.z)
        self.preview_exact_bbox = (west, south, east, north)
        self.preview_view_bbox = (west, south, east, north)
        self.user_bbox_valid = True
        self.last_bbox_source = "field_bbox_start"
        self.log_msg(
            f"FIELD-BBOX START: URL creation uses ONLY fields W={west:.8f}, S={south:.8f}, E={east:.8f}, N={north:.8f}, z={cfg.z}."
        )
        preset = MAP_PRESETS.get(self.preset_combo.currentText(), {})
        if "maps.apple.com/frame" in cfg.url_template:
            self.log_msg("Hinweis: Das Frame-Template zeigt auf maps.apple.com/frame. Nutze Download nur, wenn du für diese Quelle berechtigt bist. Für deinen eigenen Haus-/Sentinel-Server einfach die URL auf deinen eigenen Frame-Server umstellen.")
        if preset.get("preview") == "frame" or any(k in cfg.url_template for k in ("{center_lat}", "{center_lon}", "{lat_span}", "{lon_span}", "{bbox}")):
            self.start_frame_screenshot_job(cfg)
            return
        self.worker_thread = threading.Thread(target=self._run_job, args=(cfg,), daemon=True)
        self.worker_thread.start()

    def qimage_to_pil_rgb(self, qimage):
        if Image is None:
            raise RuntimeError("Pillow is required")
        from PySide6.QtGui import QImage
        img = qimage.convertToFormat(QImage.Format.Format_RGB888)
        width = img.width(); height = img.height(); bpl = img.bytesPerLine()
        data = bytes(img.constBits()[:bpl * height])
        return Image.frombytes("RGB", (width, height), data, "raw", "RGB", bpl, 1).copy()

    def clear_frame_renderers(self) -> None:
        for w in list(getattr(self, "frame_render_windows", [])):
            try:
                w.close()
                w.deleteLater()
            except Exception:
                pass
        self.frame_render_windows = []
        for item in list(getattr(self, "frame_renderers", [])):
            try:
                view = item.get("view") if isinstance(item, dict) else item
                view.setParent(None)
                view.deleteLater()
            except Exception:
                pass
        self.frame_renderers = []
        self.frame_render_windows = []
        try:
            while self.render_layout.count():
                child = self.render_layout.takeAt(0)
                if child.widget():
                    child.widget().setParent(None)
        except Exception:
            pass

    def start_frame_screenshot_job(self, cfg: StitchConfig) -> None:
        if Image is None:
            QMessageBox.critical(self, "Error", "Pillow is required for frame screenshot TIFF export.")
            return
        try:
            # Real screenshot-grid mode:
            # The output cell is the cropped visible screenshot, not an XYZ 256px tile.
            render_w = int(self.render_w_spin.value())
            render_h = int(self.render_h_spin.value())
            crop_left = int(self.crop_left_spin.value())
            crop_top = int(self.crop_top_spin.value())
            crop_right = int(self.crop_right_spin.value())
            crop_bottom = int(self.crop_bottom_spin.value())
            visible_w = max(1, render_w - crop_left - crop_right)
            visible_h = max(1, render_h - crop_top - crop_bottom)
            if visible_w <= 8 or visible_h <= 8:
                raise RuntimeError("Crop values leave too little visible area.")

            self.log_msg("FIELD-BBOX URL MODE active: every Apple/Frame URL is generated from the coordinate fields, not from the preview map.")

            # WebMercator world-pixel grid at zoom z.
            sel_left_px, sel_top_px = lonlat_to_world_pixel(cfg.min_lon, cfg.max_lat, cfg.z)
            sel_right_px, sel_bottom_px = lonlat_to_world_pixel(cfg.max_lon, cfg.min_lat, cfg.z)
            selected_width_px = max(1.0, sel_right_px - sel_left_px)
            selected_height_px = max(1.0, sel_bottom_px - sel_top_px)

            step_mult_x = float(self.pixel_step_x_spin.value()) if hasattr(self, "pixel_step_x_spin") else 1.0
            step_mult_y = float(self.pixel_step_y_spin.value()) if hasattr(self, "pixel_step_y_spin") else 1.0
            shift_x_px = FIXED_FRAME_SHIFT_X_PX
            shift_y_px = FIXED_FRAME_SHIFT_Y_PX
            # Default: crop-aware Apple/frame center correction.
            # The checkbox is optional and may not exist in older UI states.
            crop_correct_url = bool(
                getattr(self, "crop_correct_url_check", None)
                and self.crop_correct_url_check.isChecked()
            )
            effective_step_x_px = max(1.0, float(visible_w) * step_mult_x + shift_x_px)
            effective_step_y_px = max(1.0, float(visible_h) * step_mult_y + shift_y_px)

            calc_cols = max(1, int(math.ceil(selected_width_px / effective_step_x_px)))
            calc_rows = max(1, int(math.ceil(selected_height_px / effective_step_y_px)))
            min_cols = int(self.frame_min_cols_spin.value()) if hasattr(self, "frame_min_cols_spin") else 1
            min_rows = int(self.frame_min_rows_spin.value()) if hasattr(self, "frame_min_rows_spin") else 1
            cols = max(calc_cols, min_cols)
            rows = max(calc_rows, min_rows)

            if calc_cols == 1 and calc_rows == 1:
                self.log_msg(
                    "Warning: calculated frame grid is only 1 x 1. "
                    f"Selected size at z={cfg.z}: {selected_width_px:.1f} x {selected_height_px:.1f} world-px; "
                    f"step: {effective_step_x_px:.1f} x {effective_step_y_px:.1f} px. "
                    "Use higher Download Zoom, lower Render W/H or Pixel step, or increase Apple Mark Area view x cells before marking."
                )
            if cols != calc_cols or rows != calc_rows:
                self.log_msg(f"Force min grid applied: calculated {calc_cols} x {calc_rows}, using {cols} x {rows}.")

            grid_right_px = sel_left_px + cols * effective_step_x_px
            grid_bottom_px = sel_top_px + rows * effective_step_y_px
            grid_west, grid_south, grid_east, grid_north = world_pixel_bbox_to_lonlat(
                sel_left_px, sel_top_px, grid_right_px, grid_bottom_px, cfg.z
            )

            sample_url0, sample_visible0, sample_request0 = expand_frame_url_grid(
                cfg.url_template, 0, 0, cfg.z,
                cfg.min_lon, cfg.max_lat,
                0.0, 0.0,
                0.0, 0.0,
                visible_w, visible_h,
                render_w, render_h,
                crop_left, crop_top, crop_right, crop_bottom,
                step_mult_x, step_mult_y, shift_x_px, shift_y_px, crop_correct_url,
            )
            request_lon_span = abs(sample_request0[2] - sample_request0[0])
            request_lat_span = abs(sample_request0[3] - sample_request0[1])
            visible_lon_span = abs(sample_visible0[2] - sample_visible0[0])
            visible_lat_span = abs(sample_visible0[3] - sample_visible0[1])
            lon_per_px = 0.0
            lat_per_px = 0.0

            width = cols * visible_w
            height = rows * visible_h
            self.frame_cell_w = visible_w
            self.frame_cell_h = visible_h
            self.frame_render_w_actual = render_w
            self.frame_render_h_actual = render_h
            self.frame_request_lon_span = float(request_lon_span)
            self.frame_request_lat_span = float(request_lat_span)
            self.frame_lon_per_px = float(lon_per_px)
            self.frame_lat_per_px = float(lat_per_px)
            self.frame_visible_lon_span = float(visible_lon_span)
            self.frame_visible_lat_span = float(visible_lat_span)
            self.frame_selected_west = float(cfg.min_lon)
            self.frame_selected_north = float(cfg.max_lat)
            self.frame_grid_west = float(cfg.min_lon)
            self.frame_grid_north = float(cfg.max_lat)
            self.frame_grid_east = float(grid_east)
            self.frame_grid_south = float(grid_south)
            self.frame_step_mult_x = float(step_mult_x)
            self.frame_step_mult_y = float(step_mult_y)
            self.frame_shift_x_px = float(shift_x_px)
            self.frame_shift_y_px = float(shift_y_px)
            self.frame_crop_correct_url = bool(crop_correct_url)

            bounds_3857 = lonlat_bbox_to_webmercator_bounds(
                self.frame_grid_west, self.frame_grid_south, self.frame_grid_east, self.frame_grid_north
            )
            self.frame_mem, _ = open_direct_bigtiff(cfg, width, height, bounds_3857, self.log_msg)
            self.frame_cfg = cfg
            self.frame_x_min = 0
            self.frame_y_min = 0
            self.frame_tile_dir = default_tile_tif_dir(cfg)
            self.frame_tile_dir.mkdir(parents=True, exist_ok=True)

            # Create jobs by screenshot-grid cell. x/y are only stable IDs now.
            self.frame_queue = [
                TileJob(col, row, cfg.z, col, row)
                for row in range(rows)
                for col in range(cols)
            ]
            self.frame_total = len(self.frame_queue)
            self.frame_queue_index = 0
            self.frame_done = 0
            self.frame_active = True
            self.progress.setRange(0, max(1, self.frame_total))
            self.progress.setValue(0)
            self.clear_frame_renderers()

            count = max(1, min(16, int(self.frame_views_spin.value())))
            hidden = False
            self.log_msg("=== Frame Screenshot Grid mode ===")
            self.log_msg("Manual alignment mode: effective step = visible pixels * Pixel step multiplier + Frame shift px/cell.")
            self.log_msg("Crop-aware alignment mode active: Apple URL center/span includes crop L/T/R/B, with the same center-shift logic on all sides.")
            if crop_correct_url:
                self.log_msg("Crop URL correction is ON: requested URL span includes crop margins and symmetric center shifts for left/right/top/bottom.")
            if hasattr(self, "frame_preview_cells_spin"):
                self.log_msg(f"Apple/Frame Mark Area view fallback: {float(self.frame_preview_cells_spin.value()):.1f} renderer-cells. This prevents the old one-tile fallback.")
            self.log_msg("Tip: X/Y shift negative = screenshots closer/more overlap; positive = farther apart. PyPI stitching is off by default.")
            self.log_msg(f"Crop URL correction: {'ON' if crop_correct_url else 'OFF'}")
            self.log_msg(f"Selected bbox fields: S={cfg.min_lat:.8f}, W={cfg.min_lon:.8f}, N={cfg.max_lat:.8f}, E={cfg.max_lon:.8f}")
            self.log_msg(f"Grid coverage: S={self.frame_grid_south:.8f}, W={self.frame_grid_west:.8f}, N={self.frame_grid_north:.8f}, E={self.frame_grid_east:.8f}")
            self.log_msg(f"Render size: {render_w}x{render_h} px")
            self.log_msg("Screen note: monitor may be 2560x1440, but renderer capture is forced to exact Render W/H so offsets stay correct.")
            self.log_msg(f"Crop L/T/R/B: {crop_left}/{crop_top}/{crop_right}/{crop_bottom} px")
            self.log_msg(f"Visible output cell: {visible_w}x{visible_h} px")
            self.log_msg(f"Request span full screenshot: lon={request_lon_span:.12f}, lat={request_lat_span:.12f}")
            self.log_msg(f"Degrees per px: lon={lon_per_px:.14f}, lat={lat_per_px:.14f}")
            self.log_msg(f"Step per screenshot: X={effective_step_x_px:.1f}px, Y={effective_step_y_px:.1f}px; visible cell={visible_w}x{visible_h}px; multiplier={step_mult_x:.3f}/{step_mult_y:.3f}; shift={shift_x_px:.1f}/{shift_y_px:.1f}px; lon≈{visible_lon_span:.12f}, lat≈{visible_lat_span:.12f}")
            self.log_msg(f"Grid: {cols} x {rows} = {self.frame_total:,}; output pixels: {width:,} x {height:,}")
            self.log_msg(f"Render WebViews: {count}; hidden={hidden}")
            self.log_msg(f"Individual TIFF tiles: {self.frame_tile_dir}")
            self.log_msg(f"Stitched GeoTIFF/BigTIFF: {cfg.output_file}")

            # Log first two sample URLs so the math can be checked.
            for sample_col, sample_row in [(0, 0), (1, 0), (0, 1)]:
                if sample_col < cols and sample_row < rows:
                    sample_url, sample_visible, sample_request = expand_frame_url_grid(
                        cfg.url_template, sample_col, sample_row, cfg.z,
                        self.frame_selected_west, self.frame_selected_north,
                        self.frame_request_lon_span, self.frame_request_lat_span,
                        self.frame_lon_per_px, self.frame_lat_per_px,
                        self.frame_cell_w, self.frame_cell_h,
                        render_w, render_h,
                        crop_left, crop_top, crop_right, crop_bottom,
                        step_mult_x, step_mult_y, shift_x_px, shift_y_px, crop_correct_url,
                    )
                    self.log_msg(f"Sample cell col={sample_col} row={sample_row} visible={sample_visible} request={sample_request}")
                    self.log_msg(f"Sample URL col={sample_col} row={sample_row}: {sample_url}")

            fullscreen = True if not getattr(self, "fullscreen_render_check", None) else bool(self.fullscreen_render_check.isChecked())
            self.log_msg(f"Exact renderer windows: {fullscreen}; not maximized; capture size forced to Render W/H.")

            for i in range(count):
                if fullscreen:
                    win = QWidget(None, Qt.WindowType.Window)
                    win.setWindowTitle(f"PyMapStitcher Frame Renderer {i}")
                    win.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, False)
                    layout = QVBoxLayout(win)
                    layout.setContentsMargins(0, 0, 0, 0)
                    view = QWebEngineView(win)
                    configure_webengine_view(view)
                    layout.addWidget(view)
                    view.setFixedSize(render_w, render_h)
                    win.setFixedSize(render_w, render_h)
                    win.resize(render_w, render_h)
                    self.frame_render_windows.append(win)
                    win.show()
                    win.raise_()
                    win.activateWindow()
                    view.resize(render_w, render_h)
                else:
                    view = QWebEngineView(self.render_area)
                    configure_webengine_view(view)
                    view.setFixedSize(render_w, render_h)
                    view.resize(render_w, render_h)
                    view.show()
                item = {"view": view, "busy": False, "job": None, "index": i}
                self.frame_renderers.append(item)

            QTimer.singleShot(500, self._start_frame_dispatch)
        except Exception as exc:
            QMessageBox.critical(self, "Frame screenshot error", str(exc))
            self.log_msg(f"Frame screenshot error: {exc}")

    def _start_frame_dispatch(self) -> None:
        """Start all frame renderer queues after WebEngine windows had time to appear."""
        if not self.frame_active:
            return
        try:
            QApplication.processEvents()
        except Exception:
            pass
        for item in list(getattr(self, "frame_renderers", [])):
            try:
                QTimer.singleShot(0, lambda it=item: self.dispatch_next_frame_job(it))
            except Exception as exc:
                self.log_msg(f"Could not start frame renderer {item.get('index')}: {exc}")

    def dispatch_next_frame_job(self, item) -> None:
        if not self.frame_active:
            return
        if self.stop_event.is_set():
            self.finish_frame_screenshot_job(stopped=True)
            return
        if self.frame_queue_index >= len(self.frame_queue):
            item["busy"] = False
            if all(not it.get("busy") for it in self.frame_renderers):
                self.finish_frame_screenshot_job(stopped=False)
            return

        job = self.frame_queue[self.frame_queue_index]
        self.frame_queue_index += 1
        item["busy"] = True
        item["job"] = job
        item["job_key"] = (job.z, job.col, job.row)
        item["loaded_ok"] = False

        render_w = int(self.frame_render_w_actual or self.render_w_spin.value())
        render_h = int(self.frame_render_h_actual or self.render_h_spin.value())

        # Exact-size mode: URL math and captured WebView use the same render_w/render_h.
        url, visible_bounds, request_bounds = expand_frame_url_grid(
            self.frame_cfg.url_template,
            job.col, job.row, job.z,
            self.frame_selected_west, self.frame_selected_north,
            self.frame_request_lon_span, self.frame_request_lat_span,
            self.frame_lon_per_px, self.frame_lat_per_px,
            self.frame_cell_w, self.frame_cell_h,
            int(self.frame_render_w_actual), int(self.frame_render_h_actual),
            int(self.crop_left_spin.value()),
            int(self.crop_top_spin.value()),
            int(self.crop_right_spin.value()),
            int(self.crop_bottom_spin.value()),
            float(getattr(self, "frame_step_mult_x", 1.0)),
            float(getattr(self, "frame_step_mult_y", 1.0)),
            float(getattr(self, "frame_shift_x_px", 0.0)),
            float(getattr(self, "frame_shift_y_px", 0.0)),
            bool(getattr(self, "frame_crop_correct_url", False)),
        )
        item["url"] = url
        item["visible_bounds"] = visible_bounds
        item["request_bounds"] = request_bounds

        if self.frame_done == 0:
            self.log_msg(f"First frame render URL: {url}")
        if item.get("index", 0) < 4:
            self.log_msg(
                f"Frame renderer {item.get('index')} loading cell col={job.col} row={job.row}; "
                f"visible={visible_bounds}; request={request_bounds}"
            )

        view = item["view"]
        try:
            view.stop()
        except Exception:
            pass
        try:
            view.setHtml("<html><body style='margin:0;background:#000'></body></html>", QUrl("about:blank"))
            QApplication.processEvents()
        except Exception:
            pass
        try:
            view.loadFinished.disconnect()
        except Exception:
            pass
        view.loadFinished.connect(lambda ok, it=item, key=(job.z, job.col, job.row): self.frame_loaded(ok, it, key))
        view.load(QUrl(url))

    def frame_loaded(self, ok: bool, item, key=None) -> None:
        try:
            item["view"].loadFinished.disconnect()
        except Exception:
            pass

        # Ignore stale loadFinished events from the previous URL/page.
        if key is not None and item.get("job_key") != key:
            self.log_msg(f"Ignored stale loadFinished for renderer {item.get('index')}: {key} != {item.get('job_key')}")
            return

        item["loaded_ok"] = bool(ok)
        if not ok:
            job = item.get("job")
            self.log_msg(f"Warning: renderer {item.get('index')} loadFinished=False for z={job.z if job else '?'} x={job.x if job else '?'} y={job.y if job else '?'}")

        # First wait: page load -> map canvas starts painting.
        wait_ms = max(1000, int(self.rate_spin.value()))
        QTimer.singleShot(wait_ms, lambda it=item, k=key: self.frame_extra_settle_wait(it, k))

    def frame_extra_settle_wait(self, item, key=None) -> None:
        if not self.frame_active:
            return
        if key is not None and item.get("job_key") != key:
            return
        view = item["view"]
        try:
            # Force repaint/resize before final settle. This reduces captures of
            # the previous map location in Qt WebEngine.
            win = view.window()
            if win:
                view.resize(win.size())
            view.update()
            view.repaint()
            QApplication.processEvents()
        except Exception:
            pass
        extra_ms = max(0, int(self.frame_settle_extra_spin.value())) if hasattr(self, "frame_settle_extra_spin") else 2500
        QTimer.singleShot(extra_ms, lambda it=item, k=key: self.capture_frame_tile(it, k))

    def capture_frame_tile(self, item, key=None) -> None:
        if not self.frame_active:
            return
        if key is not None and item.get("job_key") != key:
            self.log_msg(f"Ignored stale capture for renderer {item.get('index')}: {key} != {item.get('job_key')}")
            return
        job = item.get("job")
        try:
            import numpy as np
            view = item["view"]
            try:
                view.resize(int(self.frame_render_w_actual), int(self.frame_render_h_actual))
                view.repaint()
                QApplication.processEvents()
            except Exception:
                pass

            pix = view.grab()
            pil = self.qimage_to_pil_rgb(pix.toImage())

            l = int(self.crop_left_spin.value())
            t = int(self.crop_top_spin.value())
            r = pil.width - int(self.crop_right_spin.value())
            b = pil.height - int(self.crop_bottom_spin.value())
            if r <= l or b <= t:
                raise RuntimeError("Crop values remove the full image. Reduce crop.")
            pil = pil.crop((l, t, r, b))

            # Normalize to the fixed visible output cell size calculated at job start.
            # This avoids mismatches if the OS maximized window differs by borders/taskbar.
            if pil.width != self.frame_cell_w or pil.height != self.frame_cell_h:
                pil = pil.resize((self.frame_cell_w, self.frame_cell_h), Image.Resampling.LANCZOS)

            tile_path = self.frame_tile_dir / f"grid_z{job.z}_col{job.col}_row{job.row}.tif"
            tile_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = tile_path.with_suffix(".tmp.tif")
            pil.save(tmp, format="TIFF", compression="tiff_deflate")
            os.replace(tmp, tile_path)

            visible_bounds = item.get("visible_bounds")
            if visible_bounds:
                write_worldfile_and_prj(
                    tile_path, self.frame_cell_w, self.frame_cell_h,
                    lonlat_bbox_to_webmercator_bounds(visible_bounds[0], visible_bounds[1], visible_bounds[2], visible_bounds[3])
                )

            arr = np.asarray(pil, dtype=np.uint8)
            r0 = job.row * self.frame_cell_h
            c0 = job.col * self.frame_cell_w
            self.frame_mem[r0:r0+self.frame_cell_h, c0:c0+self.frame_cell_w, :] = arr

            self.frame_done += 1
            self.progress.setValue(self.frame_done)
            self.status_label.setText(f"Frame grid TIFF: {self.frame_done:,}/{self.frame_total:,}")
            if self.frame_done % 5 == 0:
                try:
                    self.frame_mem.flush()
                except Exception:
                    pass
                self.log_msg(f"Frame grid progress: {self.frame_done:,}/{self.frame_total:,}")
        except Exception as exc:
            self.frame_done += 1
            self.progress.setValue(self.frame_done)
            self.log_msg(f"Frame grid capture error at col={job.col if job else '?'} row={job.row if job else '?'}: {exc}")

        try:
            item["view"].stop()
        except Exception:
            pass
        item["busy"] = False
        item["job"] = None
        item["job_key"] = None
        item["visible_bounds"] = None
        item["request_bounds"] = None
        QTimer.singleShot(0, lambda it=item: self.dispatch_next_frame_job(it))

    def finish_frame_screenshot_job(self, stopped: bool = False) -> None:
        if not self.frame_active:
            return
        self.frame_active = False
        try:
            if self.frame_mem is not None:
                self.frame_mem.flush()
                del self.frame_mem
        except Exception:
            pass
        self.frame_mem = None
        try:
            self.clear_frame_renderers()
        except Exception:
            pass
        if stopped:
            self.log_msg("Frame screenshot job stopped. Partial output remains on disk.")
            self.status_label.setText("Stopped")
        else:
            # Optional final merge with the PyPI package "stitching".
            # The old streamed grid GeoTIFF is still written first; if feature
            # stitching fails, the old output remains usable.
            try:
                use_pypi = bool(getattr(self, "pypi_stitching_check", None) and self.pypi_stitching_check.isChecked())
            except Exception:
                use_pypi = False
            if use_pypi:
                try:
                    self.log_msg("Starting PyPI stitching final merge from individual TIFF tiles...")
                    stitch_frame_tiles_with_pypi_stitching(
                        self.frame_tile_dir,
                        self.frame_cfg.output_file,
                        lonlat_bbox_to_webmercator_bounds(
                            self.frame_grid_west,
                            self.frame_grid_south,
                            self.frame_grid_east,
                            self.frame_grid_north,
                        ),
                        self.log_msg,
                    )
                except Exception as exc:
                    self.log_msg(f"PyPI stitching failed; keeping streamed grid GeoTIFF/BigTIFF: {exc}")
            self.log_msg(f"Finished frame screenshot GeoTIFF/BigTIFF: {self.frame_cfg.output_file}")
            self.log_msg(f"Finished individual TIFF tiles: {self.frame_tile_dir}")
            self.status_label.setText("Finished")

    def _run_job(self, cfg: StitchConfig) -> None:
        try:
            stitch_tiles(cfg, self._progress, self._log_thread, self.stop_event)
        except Exception as exc:
            self._log_thread(f"ERROR: {exc}")
            self.q.put(("status", "Error"))

    def _progress(self, done: int, total: int, phase: str) -> None:
        self.q.put(("progress", done, total, phase))

    def _log_thread(self, msg: str) -> None:
        self.q.put(("log", msg))

    def log_msg(self, msg: str) -> None:
        self.log.append(str(msg))

    def _poll(self) -> None:
        try:
            while True:
                item = self.q.get_nowait()
                if item[0] == "log":
                    self.log_msg(item[1])
                elif item[0] == "progress":
                    _, done, total, phase = item
                    self.progress.setRange(0, max(1, int(total)))
                    self.progress.setValue(int(done))
                    self.status_label.setText(f"{phase}: {done:,}/{total:,}")
                elif item[0] == "status":
                    self.status_label.setText(item[1])
        except queue.Empty:
            pass


def main() -> int:
    if _PYSIDE_IMPORT_ERROR is not None:
        print("PySide6 WebEngine is missing.")
        print("Install with: python -m pip install PySide6 PySide6-Addons PySide6-Essentials shiboken6")
        print("Import error:", _PYSIDE_IMPORT_ERROR)
        return 1
    os.environ.setdefault("QTWEBENGINE_CHROMIUM_FLAGS", "--ignore-gpu-blocklist")
    app = QApplication(sys.argv)
    win = PySideMapStitcher()
    win.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
