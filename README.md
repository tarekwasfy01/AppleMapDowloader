# QGIS Plugin Repository / Upload Checklist

- ZIP contains exactly one top-level folder: `AppleMapDownloaderQGIS/`.
- Top-level plugin folder contains `metadata.txt`, `__init__.py`, and `LICENSE` without extension.
- No `__pycache__`, `.pyc`, hidden macOS files, local virtual environments, build folders, or bundled binaries are included.
- Metadata uses an English description and a unique version number: `0.2.5`.
- Metadata includes `license=GNU GPL v2 or later`.
- Public metadata links are set for homepage, tracker, and repository.
- Before upload, push this exact plugin source to the public repository listed in `metadata.txt`.
- The icon is included as `icon.png` and referenced in `metadata.txt`.
- Tested target metadata: QGIS `3.22` through `3.99`; QGIS 4 compatibility is not claimed.
- Plugin uses only Apple Maps frame rendering through an external Chromium-compatible browser backend.
- Plugin does not bundle browser binaries, font files, credentials, API keys, or private user data.
- External requirement is documented: Microsoft Edge, Google Chrome, Chromium, Brave, or Vivaldi must be installed separately.
