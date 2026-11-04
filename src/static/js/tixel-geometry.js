(function (root) {
  'use strict';
  const presets = {10: {width_um: 10, gap_um: 15}, 15: {width_um: 15, gap_um: 10}, 25: {width_um: 25, gap_um: 25}, 50: {width_um: 50, gap_um: 50}};
  function size(metadata) {
    const value = parseFloat(metadata.chip_resolution);
    return presets[value] ? value : 25;
  }
  function fromMetadata(metadata) {
    const preset = presets[size(metadata)];
    const custom = metadata.tixel_geometry || preset;
    return {width_um: custom.width_um == null ? NaN : Number(custom.width_um), gap_um: custom.gap_um == null ? NaN : Number(custom.gap_um)};
  }
  function valid(metadata) {
    const g = fromMetadata(metadata);
    return Number.isFinite(g.width_um) && g.width_um > 0 && Number.isFinite(g.gap_um) && g.gap_um >= 0;
  }
  function vectors(start, end, n, geometry) {
    if (!Number.isInteger(n) || n < 1 || !valid({tixel_geometry: geometry})) throw new Error('Invalid tixel geometry');
    const span = n * geometry.width_um + (n - 1) * geometry.gap_um;
    const edge = [end.x - start.x, end.y - start.y];
    return {capture: edge.map(v => v * geometry.width_um / span), pitch: edge.map(v => v * (geometry.width_um + geometry.gap_um) / span)};
  }
  function add(a, b) { return [a[0] + b[0], a[1] + b[1]]; }
  function length(v) { return Math.hypot(v[0], v[1]); }
  function element(id) { return root.document && root.document.getElementById(id + '_tixel'); }
  function loadTixels(rows) {
    rows = rows || [];
    const points = this.getCoordinates();
    const horizontal = vectors(points[0], points[1], this.channels, this._tixelGeometry);
    const vertical = vectors(points[0], points[3], this.channels, this._tixelGeometry);
    this.radius = Math.min(length(horizontal.capture), length(vertical.capture)) / 2;
    this.polygons = [];
    for (let col = 0; col < this.channels; col++) {
      for (let row = 0; row < this.channels; row++) {
        const origin = [points[0].x + col * horizontal.pitch[0] + row * vertical.pitch[0], points[0].y + col * horizontal.pitch[1] + row * vertical.pitch[1]];
        const tr = add(origin, horizontal.capture), bl = add(origin, vertical.capture), br = add(tr, vertical.capture);
        const center = add(origin, add(horizontal.capture, vertical.capture).map(v => v / 2));
        const id = col * this.channels + row;
        const fill = rows[id] && String(rows[id][1]) === '1' ? 'red' : 'none';
        this.polygons.push([origin, tr, br, bl, id, 1, center[0], center[1], fill, [row, col]]);
        const node = element(id);
        if (node) {
          node.setAttribute('points', [origin, tr, br, bl].flat().join(','));
          node.setAttribute('fill', fill);
        }
      }
    }
  }
  function getQCScaleFactors(img) {
    const p = this.getCoordinates();
    const h = vectors(p[0], p[1], this.channels, this._tixelGeometry).capture;
    const v = vectors(p[0], p[3], this.channels, this._tixelGeometry).capture;
    // Preserve the browser's existing equivalent-diameter convention.
    const diameter = (length(add(h, v)) + length([h[0] - v[0], h[1] - v[1]])) / (2 * this.scalefactor);
    return {spot_diameter_fullres: diameter, fiducial_diameter_fullres: diameter * 1.6153846, tissue_hires_scalef: 2000 / img.image.width, tissue_lowres_scalef: 600 / img.image.width};
  }
  function autoMask(pixels, threshold) {
    const height = pixels.shape[0], width = pixels.shape[1];
    this.polygons.forEach(poly => {
      const node = element(poly[4]);
      const points = node ? node.getAttribute('points').split(',').map(Number) : poly.slice(0, 4).flat();
      const origin = [points[0] / this.scalefactor, points[1] / this.scalefactor];
      const h = [(points[2] - points[0]) / this.scalefactor, (points[3] - points[1]) / this.scalefactor];
      const v = [(points[6] - points[0]) / this.scalefactor, (points[7] - points[1]) / this.scalefactor];
      const nx = Math.max(1, Math.ceil(length(h))), ny = Math.max(1, Math.ceil(length(v)));
      let sum = 0, count = 0;
      for (let i = 0; i < nx; i++) for (let j = 0; j < ny; j++) {
        const x = Math.floor(origin[0] + (i + 0.5) / nx * h[0] + (j + 0.5) / ny * v[0]);
        const y = Math.floor(origin[1] + (i + 0.5) / nx * h[1] + (j + 0.5) / ny * v[1]);
        if (x >= 0 && x < width && y >= 0 && y < height) { sum += pixels.data[y * width + x]; count++; }
      }
      poly[8] = count && sum / count < threshold ? 'red' : 'none';
      if (node) node.setAttribute('fill', poly[8]);
    });
    return this.polygons;
  }
  function bind(roi, metadata) {
    if (!valid(metadata)) throw new Error('Capture width must be positive and gap must be nonnegative.');
    const geometry = fromMetadata(metadata);
    const changed = JSON.stringify(geometry) !== JSON.stringify(roi._tixelGeometry);
    roi.loadTixels = loadTixels;
    roi.getQCScaleFactors = getQCScaleFactors;
    roi.autoMask = autoMask;
    roi._tixelGeometry = geometry;
    metadata.chip_resolution = size(metadata) + 'um';
    metadata.tixel_geometry = geometry;
    if (changed && roi.polygons.length) {
      const rows = roi.polygons.map(p => [null, (element(p[4]) ? element(p[4]).getAttribute('fill') : p[8]) === 'red' ? '1' : '0']);
      roi.loadTixels(rows);
    }
    return roi;
  }
  function render(vm, h) {
    const metadata = vm.metadata, geometry = fromMetadata(metadata);
    function select(value) {
      vm.$set(metadata, 'chip_resolution', value + 'um');
      vm.$set(metadata, 'tixel_geometry', Object.assign({}, presets[value]));
    }
    function input(key, value) {
      const next = Object.assign({}, metadata.tixel_geometry || geometry);
      next[key] = value === '' ? null : Number(value);
      vm.$set(metadata, 'tixel_geometry', next);
    }
    function field(label, key, min) {
      return h('label', {style: {display: 'block', margin: '12px 0'}}, [label, h('input', {attrs: {type: 'number', min: min, step: 'any', required: true, 'aria-label': label}, domProps: {value: metadata.tixel_geometry ? metadata.tixel_geometry[key] : geometry[key]}, style: {display: 'block', border: '1px solid #999', padding: '8px', width: '100%', borderRadius: '4px'}, on: {input: event => input(key, event.target.value)}})]);
    }
    const controls = [h('v-select', {attrs: {outlined: true, dense: true, label: 'Tixel size (µm)', items: [10, 15, 25, 50]}, props: {value: size(metadata)}, on: {input: select}}), h('details', {style: {marginBottom: '16px'}}, [h('summary', {style: {cursor: 'pointer'}}, ['Override tixel geometry']), field('Capture width (µm)', 'width_um', 0), field('Gap (µm)', 'gap_um', 0), h('p', [valid(metadata) ? 'Center-to-center pitch: ' + (geometry.width_um + geometry.gap_um) + ' µm' : 'Enter a positive capture width and a nonnegative gap.']), h('button', {attrs: {type: 'button'}, style: {textDecoration: 'underline'}, on: {click: () => select(size(metadata))}}, ['Restore defaults'])])];
    return h('div', controls);
  }
  const api = {presets, size, fromMetadata, valid, vectors, bind, render};
  root.AtlasTixelGeometry = api;
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
})(typeof window !== 'undefined' ? window : globalThis);
