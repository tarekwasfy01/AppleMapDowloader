# Apple Maps Downloader QGIS Plugin

This QGIS plugin lets you select a rectangular area on the QGIS map canvas and render Apple Maps satellite frame views into an EPSG:3857 GeoTIFF/BigTIFF.

## Version 0.2.5

This version does **not** require QtWebEngine, QWebEngineView, QtWebKit, or QWebView inside QGIS. QGIS still uses PyQt for the plugin user interface, but Apple Maps rendering is delegated to an external Chromium-compatible browser:

- Microsoft Edge
- Google Chrome
- Chromium
- Brave
- Vivaldi

The browser capture size is fixed to **1600 x 1600 pixels**. Each screenshot is then cropped before it is written to the GeoTIFF: **left 300 px**, **top 0 px**, **right 100 px**, **bottom 100 px**. The effective saved cell size is therefore **1200 x 1500 pixels**.

## Usage

1. Install the ZIP in QGIS via **Plugins > Manage and Install Plugins > Install from ZIP**.
2. Open **Plugins > Apple Maps Downloader** or click the toolbar icon.
3. Click **Select area by dragging a rectangle**.
4. Drag a rectangle in the QGIS map canvas.
5. Choose an output `.tif` path.
6. Make sure a Chromium-compatible browser executable is detected or selected.
7. Click **Render Apple Maps to GeoTIFF**.

## Rendering backend

The plugin uses the external browser in headless screenshot mode. It avoids missing Qt WebEngine problems in QGIS builds where `qgis.PyQt.QtWebEngineWidgets` is unavailable.

## Output

- EPSG:3857 / Web Mercator GeoTIFF
- BigTIFF when required
- Tiled DEFLATE-compressed RGB raster
- Optional automatic load into QGIS

## Legal / service note

Use this only with services/content for which you have permission. The plugin renders browser pages and captures them; it does not bypass authentication, access controls, or service terms.


Fixed frame shift: **X 260 px** and **Y 330 px**. With the fixed crop, the saved cell remains **1200 x 1500 px**, while the Apple frame center step is **1460 x 1830 px**.


## QGIS Plugin Repository upload notes

This ZIP is prepared for the official QGIS Plugin Repository. The plugin folder contains `metadata.txt`, `__init__.py`, `LICENSE`, `README.md`, `icon.png`, and the Python source. Before upload, push the same source code to the public repository listed in `metadata.txt` so the repository source and uploaded ZIP match.

Tested target metadata: QGIS 3.22 to 3.99. QGIS 4 compatibility is not claimed in this package.
