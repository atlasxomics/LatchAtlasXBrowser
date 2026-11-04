const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const geometry = require('../src/static/js/tixel-geometry.js');
const nodes = new Map();
global.document = {getElementById: id => nodes.get(id)};
function node() {return {attrs: {}, getAttribute(k) {return this.attrs[k];}, setAttribute(k, v) {this.attrs[k] = String(v);}};}
function roi(n, width, height) {
  return {channels: n, scalefactor: 1, polygons: [], getCoordinates: () => [{x: 0, y: 0}, {x: width, y: 0}, {x: width, y: height}, {x: 0, y: height}]};
}
function near(actual, expected) {assert.ok(Math.abs(actual - expected) < 1e-8, `${actual} != ${expected}`);}
for (const n of [1, 50, 96]) for (const size of [10, 15, 25, 50]) {
  const preset = geometry.presets[size];
  const span = n * preset.width_um + (n - 1) * preset.gap_um;
  const r = roi(n, span, span), metadata = {chip_resolution: size + 'um'};
  geometry.bind(r, metadata).loadTixels();
  assert.equal(r.polygons.length, n * n);
  near(r.polygons[0][1][0], preset.width_um);
  near(r.polygons.at(-1)[2][0], span);
  near(r.polygons.at(-1)[2][1], span);
  if (n > 1) near(r.polygons[n][6] - r.polygons[0][6], preset.width_um + preset.gap_um);
  const restored = geometry.bind(roi(n, span, span), JSON.parse(JSON.stringify(metadata)));
  restored.loadTixels();
  assert.deepEqual(restored.polygons, r.polygons);
}
// Rotation and unequal image-axis lengths preserve physical geometry ratios.
const custom = {width_um: 12, gap_um: 8};
const axis = geometry.vectors({x: 3, y: 4}, {x: 63, y: 84}, 3, custom);
near(axis.capture[0], 60 * 12 / 52); near(axis.pitch[1], 80 * 20 / 52);
for (const g of [{width_um: 0, gap_um: 1}, {width_um: 10, gap_um: -1}, {width_um: NaN, gap_um: 1}, {width_um: 10, gap_um: null}]) assert.equal(geometry.valid({tixel_geometry: g}), false);
assert.equal(geometry.valid({tixel_geometry: {width_um: 10, gap_um: 0}}), true);
// Reconfiguring a rendered grid updates SVG centers while retaining tissue labels.
const r = roi(2, 35, 35), metadata = {chip_resolution: '10um'};
geometry.bind(r, metadata).loadTixels();
r.polygons.forEach(p => {const el = node(); el.setAttribute('points', p.slice(0, 4).flat().join(',')); el.setAttribute('fill', p[4] === 1 ? 'red' : 'none'); nodes.set(p[4] + '_tixel', el);});
metadata.chip_resolution = '15um'; delete metadata.tixel_geometry;
geometry.bind(r, metadata);
assert.equal(nodes.get('1_tixel').getAttribute('fill'), 'red');
near(r.polygons[0][1][0], 35 * 15 / 40);
// Exercise the shipped getMask method against updated SVGs, as the CSV exporter does.
const bundle = fs.readFileSync(path.join(__dirname, '../src/static/js/app.d453173a.js'), 'utf8');
const start = bundle.indexOf('key:"getMask",value:') + 'key:"getMask",value:'.length;
const end = bundle.indexOf('},{key:', start);
const center = (a,b,c,d) => [(a[0]+b[0]+c[0]+d[0])/4, (a[1]+b[1]+c[1]+d[1])/4];
const getMask = new Function('e', 'return (' + bundle.slice(start,end) + ')')({center});
const mask = getMask.call(r, []);
near(mask[0].coordinates.x, r.polygons[0][6]);
assert.equal(mask[1].value, 1);
// Classify only within captures: dark gaps must not mark bright captures on tissue.
nodes.clear();
const sample = roi(2, 35, 35); geometry.bind(sample, {chip_resolution:'10um'}).loadTixels();
const pixels = {shape:[35,35], data: new Uint8Array(35*35)};
for (let y=0;y<35;y++) for(let x=0;x<35;x++) if((x<10||x>=25)&&(y<10||y>=25)) pixels.data[y*35+x]=255;
sample.autoMask(pixels, 128);
assert.ok(sample.polygons.every(p=>p[8]==='none'));
assert.ok(Number.isFinite(sample.getQCScaleFactors({image:{width:35}}).spot_diameter_fullres));
// UI selection resets overrides; fields validate, expose pitch, and restore defaults.
const vm = {metadata:{}, $set:(object,key,value)=>{object[key]=value;}};
const h = (tag,data,children) => {if(Array.isArray(data)){children=data;data={};}return {tag,data:data||{},children:children||[]};};
let view=geometry.render(vm,h); view.children[0].data.on.input(10);
assert.deepEqual(vm.metadata.tixel_geometry, {width_um:10,gap_um:15});
view=geometry.render(vm,h);
view.children[1].children[2].children[1].data.on.input({target:{value:'20'}});
assert.equal(geometry.fromMetadata(vm.metadata).gap_um,20);
view=geometry.render(vm,h); assert.match(view.children[1].children[3].children[0], /30 µm/);
view.children[1].children[4].data.on.click(); assert.equal(geometry.fromMetadata(vm.metadata).gap_um,15);
view.children[0].data.on.input(50); assert.deepEqual(vm.metadata.tixel_geometry,{width_um:50,gap_um:50});
console.log('Passed presets, boundaries, rotation, overrides, persistence, SVG/export, classification, and UI checks.');
