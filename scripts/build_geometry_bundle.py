"""Patch the shipped frontend bundle with the readable tixel geometry module.

Run from any directory: python3 scripts/build_geometry_bundle.py
The original bundle and source map remain unchanged.
"""
from pathlib import Path

root = Path(__file__).resolve().parents[1]
source = root / "src/static/js/app.d453173a.js"
bundle = source.read_text()

old = 'r("selector",{attrs:{display_label:"Chip Resolution",display_options:e.drop_down_manager.chip_resolution_list},on:{"option-added":function(t){return e.updateList("chip_resolution_list",t)}},model:{value:e.metadata.chip_resolution,callback:function(t){e.$set(e.metadata,"chip_resolution",t)},expression:"metadata.chip_resolution"}})'
assert bundle.count(old) == 1, "Metadata UI changed; review the integration patch."
bundle = bundle.replace(old, 'window.AtlasTixelGeometry.render(e,r)')
old = 'disabled:!e.metadata.barcode_filename,outlined:""'
assert bundle.count(old) == 1
bundle = bundle.replace(old, 'disabled:!e.metadata.barcode_filename||!window.AtlasTixelGeometry.valid(e.metadata),outlined:""')

# Bind the current run's metadata before generating, classifying or exporting.
for method, expected in [("loadTixels", 2), ("getQCScaleFactors", 1), ("autoMask", 1), ("getMask", 1)]:
    old = f"I.value.{method}("
    assert bundle.count(old) == expected, f"Unexpected {method} call sites"
    bundle = bundle.replace(old, f"window.AtlasTixelGeometry.bind(I.value,pt.value).{method}(")

# Confirming metadata also refreshes any existing polygons while preserving labels.
old = 'function Jt(){return Gt.apply(this,arguments)}'
assert bundle.count(old) == 1
bundle = bundle.replace(old, 'function Jt(){if(!window.AtlasTixelGeometry.valid(pt.value))return;window.AtlasTixelGeometry.bind(I.value,pt.value);return Gt.apply(this,arguments)}')
bundle = bundle.replace('//# sourceMappingURL=app.d453173a.js.map', '')
(source.parent / "app.geometry.js").write_text(bundle)

template = root / "src/templates/index.html"
html = template.read_text()
html = html.replace('/static/js/app.d453173a.js', '/static/js/app.geometry.js')
if '/static/js/tixel-geometry.js' not in html:
    html = html.replace('<script src=/static/js/app.geometry.js>', '<script src=/static/js/tixel-geometry.js></script><script src=/static/js/app.geometry.js>')
template.write_text(html)
