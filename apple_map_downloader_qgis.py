# -*- coding: utf-8 -*-

def classFactory(iface):
    from .apple_map_downloader_qgis import AppleMapDownloaderQGISPlugin
    return AppleMapDownloaderQGISPlugin(iface)
