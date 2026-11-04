# LatchAtlasXBrowser

Steps:
1) Install Vim
2) update /etc/supervisor/conf.d/latch.conf
3) Create abrowser.sh in /etc
4) In the /etc folder run "bash abrowser.sh"


## Tixel geometry

The run metadata dialog provides a **Tixel size (µm)** selector and an expandable
**Override tixel geometry** section. Selecting a size resets the capture width and
gap to its defaults; custom widths must be positive and gaps nonnegative. The
center-to-center pitch is the sum of width and gap.

| Tixel size (µm) | Capture width (µm) | Gap (µm) | Pitch (µm) |
| --- | --- | --- | --- |
| 10 | 10 | 15 | 25 |
| 15 | 15 | 10 | 25 |
| 25 | 25 | 25 | 50 |
| 50 | 50 | 50 | 100 |

ROI anchors mark the outer capture-array edges. For N channels, the physical
span is `N * width + (N - 1) * gap`. The same geometry drives polygons, exported
centers, capture-size metadata and automatic tissue classification. Classification
samples the capture parallelograms and excludes the gaps. Overrides are saved in
`metadata.json` as `tixel_geometry: {width_um, gap_um}`, alongside `chip_resolution`.
Missing geometry uses the selected size's preset; missing/unsupported size uses
25 µm. Existing runs with a recognized size but no geometry use the new defaults
when reopened; previous CSVs are updated only when the user saves the run.

This repository ships a compiled frontend rather than its Vue source project.
The readable implementation lives in `src/static/js/tixel-geometry.js`.
`scripts/build_geometry_bundle.py` creates `app.geometry.js` from the original
bundle and updates the HTML entry point. It asserts the expected integration
sites so a replacement upstream bundle requires explicit review. The original
bundle and its source map are preserved; the patched bundle intentionally omits
the original source-map reference.

Rebuild and verify:

```sh
python3 scripts/build_geometry_bundle.py
node --check src/static/js/app.geometry.js
node tests/tixel-geometry.test.cjs
```
