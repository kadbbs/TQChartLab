# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path

from PyInstaller.utils.hooks import collect_data_files, collect_submodules


project_root = Path(__file__).resolve().parent

datas = [
    (str(project_root / "templates"), "templates"),
    (str(project_root / "static"), "static"),
    (str(project_root / "custom_indicators.py"), "."),
    (str(project_root / "custom_indicators.example.py"), "."),
    (str(project_root / ".env.example"), "."),
    (str(project_root / "README.md"), "."),
    (str(project_root / "LICENSE"), "."),
]

hiddenimports = []
hiddenimports += collect_submodules("tq_app")
hiddenimports += collect_submodules("tqsdk")

datas += collect_data_files("tqsdk")


a = Analysis(
    ["web_tq_chart.py"],
    pathex=[str(project_root)],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="tq-chart-workbench",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="tq-chart-workbench",
)
