// 自动为所有 @node-wot/* 包建立软链到 @yidingdian/*
const fs = require('fs');
const path = require('path');

const pkgs = [
  'core',
  'binding-coap',
  'binding-file',
  'binding-http',
  'binding-mbus',
  'binding-modbus',
  'binding-mqtt',
  'binding-netconf',
  'binding-opcua',
  'binding-queue',
  'binding-websockets',
  'browser-bundle',
  'cli',
  'examples'
];

const nodeModules = path.resolve(__dirname, '../node_modules');

pkgs.forEach(pkg => {
  const src = path.resolve(__dirname, `../packages/${pkg}`);
  const destDir = path.join(nodeModules, '@node-wot');
  const dest = path.join(destDir, pkg);
  try {
    if (!fs.existsSync(destDir)) fs.mkdirSync(destDir);
    if (fs.existsSync(dest)) fs.rmSync(dest, { recursive: true, force: true });
    fs.symlinkSync(src, dest, 'dir');
    console.log(`Linked @node-wot/${pkg} -> ${src}`);
  } catch (e) {
    console.error(`Failed to link @node-wot/${pkg}:`, e);
  }
});
