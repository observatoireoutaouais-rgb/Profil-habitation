// ─── DATA STORE ───────────────────────────────────────────────────────────────
const BASE = location.hostname === 'localhost' ? '' : '';
const DATA = {};
const TOPOS = {};

const TYPE_COLORS = {
  "Autres immeubles résidentiels":              "#8fbc8f",
  "Chalet et maison de villégiature":           "#b2deda",
  "Habitation en commun":                       "#6dbfb8",
  "Immeuble comportant deux logements ou plus": "#5a6fa0",
  "Maison mobile et roulotte":                  "#4a90a4",
  "Maisons individuelles détachées":            "#1a4e4b",
  "Maisons jumelées ou en rangée":              "#c8b89a",
  "Total des unités d'évaluation résidentielles": "#3d8f8a",
};

const FOOTNOTE = `Les types de logements sont classés selon leur utilisation prédominante, identifiée par le code d'utilisation des biens-fonds (CUBF), leur lien physique et le nombre de logements inscrits au rôle d'évaluation foncière du Québec.`;

let currentMRC     = null;
let currentTab     = 'logements';
let currentYear    = null;
let currentYearMin = null;
let currentYearMax = null;

// ─── LOAD DATA ────────────────────────────────────────────────────────────────
async function loadAll() {
  const files = [
    'logements_types_mrc','logements_types_mun',
    'valeur_mrc','valeur_mun',
    'age_mrc','age_mun',
    'periode_mrc','mrc_list'
  ];
  await Promise.all(files.map(async f => {
    const r = await fetch(`./data/${f}.json`);
    DATA[f] = await r.json();
  }));

  // Load maps
  const regions = ['Outaouais','Laurentides','Monteregie'];
  await Promise.all(regions.map(async r => {
    const res = await fetch(`./maps/${r}_web_topojson.json`);
    TOPOS[r] = await res.json();
  }));
}

function getAvailableYears() {
  const years = new Set();
  [
    DATA.logements_types_mrc,
    DATA.logements_types_mun,
    DATA.valeur_mrc,
    DATA.valeur_mun,
    DATA.age_mrc,
    DATA.age_mun,
    DATA.periode_mrc,
  ].forEach(arr => {
    (arr || []).forEach(d => {
      const year = Number(d?.Annee);
      if (Number.isFinite(year)) years.add(year);
    });
  });
  return [...years].sort((a, b) => a - b);
}

function populateYearSelect() {
  const yearSelect = document.getElementById('sel-annee');
  const minSelect = document.getElementById('sel-annee-min');
  const maxSelect = document.getElementById('sel-annee-max');
  const years = getAvailableYears();

  if (!years.length) {
    [yearSelect, minSelect, maxSelect].forEach(sel => {
      if (sel) sel.innerHTML = '<option value="">Aucune donnée</option>';
    });
    currentYear = null;
    currentYearMin = null;
    currentYearMax = null;
    return;
  }

  const earliest = years[0];
  const latest = years[years.length - 1];

  if (!years.includes(currentYear)) currentYear = latest;
  if (!years.includes(currentYearMin)) currentYearMin = earliest;
  if (!years.includes(currentYearMax)) currentYearMax = latest;
  if (currentYearMin > currentYearMax) currentYearMin = currentYearMax;
  if (currentYear < currentYearMin) currentYear = currentYearMin;
  if (currentYear > currentYearMax) currentYear = currentYearMax;

  const descOptions = years.slice().reverse()
    .map(year => `<option value="${year}">${year}</option>`)
    .join('');
  const ascOptions = years
    .map(year => `<option value="${year}">${year}</option>`)
    .join('');

  if (yearSelect) {
    yearSelect.innerHTML = descOptions;
    yearSelect.value = String(currentYear);
  }
  if (minSelect) {
    minSelect.innerHTML = ascOptions;
    minSelect.value = String(currentYearMin);
  }
  if (maxSelect) {
    maxSelect.innerHTML = ascOptions;
    maxSelect.value = String(currentYearMax);
  }
}

// ─── MRC SELECTOR ─────────────────────────────────────────────────────────────
function getFilteredYears() {
  return getAvailableYears().filter(year => {
    if (currentYearMin !== null && year < currentYearMin) return false;
    if (currentYearMax !== null && year > currentYearMax) return false;
    return true;
  });
}

function syncYearRange(changed) {
  const years = getAvailableYears();
  if (!years.length) return;

  if (changed === 'min' && currentYearMin > currentYearMax) {
    currentYearMax = currentYearMin;
  }
  if (changed === 'max' && currentYearMax < currentYearMin) {
    currentYearMin = currentYearMax;
  }

  const filteredYears = getFilteredYears();
  if (!filteredYears.length) return;

  if (!filteredYears.includes(currentYear)) {
    currentYear = filteredYears[filteredYears.length - 1];
  }

  populateYearSelect();
}

function populateMRCSelect(regionFilter) {
  const sel = document.getElementById('sel-mrc');
  const mrcList = DATA.mrc_list || [];
  const filtered = regionFilter
    ? mrcList.filter(d => String(d.Region) === regionFilter)
    : mrcList;
  sel.innerHTML = '<option value="">Toutes les MRC</option>' +
    filtered.map(d => `<option value="${d.CDNAME}">${d.CDNAME}</option>`).join('');
  if (currentMRC && filtered.find(d => d.CDNAME === currentMRC)) {
    sel.value = currentMRC;
  } else {
    currentMRC = null;
  }
}

// ─── MAP ──────────────────────────────────────────────────────────────────────
function drawMap() {
  const container = document.getElementById('map-container');
  const w = container.clientWidth;
  const h = Math.max(container.clientHeight - 10, 260);
  const svg = d3.select('#map-svg').attr('viewBox', `0 0 ${w} ${h}`);
  svg.selectAll('*').remove();

  // Merge all topojson features
  const allFeatures = [];
  for (const [name, topo] of Object.entries(TOPOS)) {
    const key = Object.keys(topo.objects)[0];
    const fc = topojson.feature(topo, topo.objects[key]);
    fc.features.forEach(f => { f.properties._region = name; allFeatures.push(f); });
  }

  const projection = d3.geoMercator().fitSize([w, h], {type:'FeatureCollection', features: allFeatures});
  const path = d3.geoPath().projection(projection);

  // Color scale based on current data
  const anneeData = (DATA.logements_types_mrc || []).filter(d => d.Annee == currentYear);
  const totals = {};
  anneeData.forEach(d => { totals[d.CDNAME] = d.Total || 0; });
  const vals = Object.values(totals).filter(v => v > 0);
  const colorScale = d3.scaleSequential()
    .domain([0, d3.max(vals) || 1])
    .interpolator(d3.interpolate('#e8f4f2', '#1a4e4b'));

  // Group by MRC
  const mrcFeatures = {};
  allFeatures.forEach(f => {
    const mrc = f.properties.nom_MRC;
    if (!mrcFeatures[mrc]) mrcFeatures[mrc] = [];
    mrcFeatures[mrc].push(f);
  });

  const g = svg.append('g');

  // Draw by municipality, colored by MRC total
  allFeatures.forEach(f => {
    const mrc = f.properties.nom_MRC;
    const total = totals[mrc] || 0;
    g.append('path')
      .datum(f)
      .attr('class', `mrc-path ${currentMRC === mrc ? 'selected' : ''}`)
      .attr('d', path)
      .attr('fill', colorScale(total))
      .attr('stroke', '#ffffff')
      .attr('stroke-width', 0.5)
      .on('click', () => selectMRC(mrc))
      .on('mouseover', (event) => showTooltip(event,
        `<strong>${f.properties.nom_MUN}</strong><br>${mrc}<br>${total.toLocaleString('fr-CA')} logements`))
      .on('mousemove', moveTooltip)
      .on('mouseout', hideTooltip);
  });

  // MRC borders
  Object.entries(mrcFeatures).forEach(([mrc, features]) => {
    const merged = topojson.merge(
      {type:'Topology', arcs:[], objects:{m:{type:'GeometryCollection', geometries:[]}}},
      []
    );
    // Simple: draw outline per mun with thicker stroke for MRC identity
    g.append('path')
      .datum({type:'FeatureCollection', features})
      .attr('d', path)
      .attr('fill','none')
      .attr('stroke', currentMRC === mrc ? '#e8a048' : '#ffffff')
      .attr('stroke-width', currentMRC === mrc ? 2 : 0.3);
  });

  // Legend
  const legendTitle = document.getElementById('legend-title');
  if (legendTitle) legendTitle.textContent = `Nombre de logements (${currentYear ?? '—'})`;
  drawLegend(colorScale, d3.max(vals) || 0);
}

function drawLegend(scale, maxVal) {
  const steps = 5;
  const scaleEl = document.getElementById('legend-scale');
  const labelsEl = document.getElementById('legend-labels');
  scaleEl.innerHTML = '';
  labelsEl.innerHTML = '';

  for (let i = 0; i < steps; i++) {
    const v = (maxVal / (steps-1)) * i;
    const div = document.createElement('div');
    div.className = 'legend-bar';
    div.style.background = scale(v);
    scaleEl.appendChild(div);
  }
  ['0', Math.round(maxVal/2).toLocaleString('fr-CA'), maxVal.toLocaleString('fr-CA')].forEach(l => {
    const s = document.createElement('span');
    s.textContent = l;
    labelsEl.appendChild(s);
  });
}

function selectMRC(mrc) {
  currentMRC = currentMRC === mrc ? null : mrc;
  document.getElementById('sel-mrc').value = currentMRC || '';
  drawMap();
  render();
}

// ─── TOOLTIP ──────────────────────────────────────────────────────────────────
function showTooltip(event, html) {
  const t = document.getElementById('tooltip');
  t.innerHTML = html;
  t.style.opacity = '1';
  moveTooltip(event);
}
function moveTooltip(event) {
  const t = document.getElementById('tooltip');
  t.style.left = (event.clientX + 12) + 'px';
  t.style.top  = (event.clientY - 10) + 'px';
}
function hideTooltip() {
  document.getElementById('tooltip').style.opacity = '0';
}

// ─── RENDER ───────────────────────────────────────────────────────────────────
function render() {
  const content = document.getElementById('content');
  const mrcLabel = currentMRC || 'Toutes les MRC';
  const annee = currentYear;

  // Filter helpers
  const filterMRC = arr => currentMRC ? arr.filter(d => d.CDNAME === currentMRC) : arr;
  const filterYear = arr => arr.filter(d => d.Annee == annee);
  const filterYearRange = arr => arr.filter(d => {
    const year = Number(d.Annee);
    if (currentYearMin !== null && year < currentYearMin) return false;
    if (currentYearMax !== null && year > currentYearMax) return false;
    return true;
  });
  const filterYearMRC = arr => filterMRC(filterYear(arr));
  const filterYearRangeMRC = arr => filterMRC(filterYearRange(arr));

  if (currentTab === 'logements') renderLogements(content, mrcLabel, annee, filterMRC, filterYear, filterYearMRC, filterYearRangeMRC);
  else if (currentTab === 'valeur') renderValeur(content, mrcLabel, annee, filterYearMRC, filterYearRangeMRC);
  else if (currentTab === 'age') renderAge(content, mrcLabel, annee, filterYearMRC, filterYearRangeMRC);
}

// ── TAB: LOGEMENTS ────────────────────────────────────────────────────────────
function renderLogements(content, mrcLabel, annee, filterMRC, filterYear, filterYearMRC, filterYearRangeMRC) {
  const mrcData = filterYearMRC(DATA.logements_types_mrc || []);
  const munData = filterYearMRC(DATA.logements_types_mun || []);

  // Aggregate if multiple MRCs
  const TYPE_COLS = [
    "Autres immeubles résidentiels","Chalet et maison de villégiature",
    "Habitation en commun","Immeuble comportant deux logements ou plus",
    "Maison mobile et roulotte","Maisons individuelles détachées","Maisons jumelées ou en rangée"
  ];

  const agg = {};
  mrcData.forEach(row => {
    TYPE_COLS.forEach(t => { agg[t] = (agg[t]||0) + (row[t]||0); });
  });
  const total = TYPE_COLS.reduce((s,t) => s+(agg[t]||0), 0);

  // Multi-year for stacked bar
  const allYears = filterYearRangeMRC(DATA.logements_types_mrc || []);
  const byYear = {};
  allYears.forEach(row => {
    if (!byYear[row.Annee]) byYear[row.Annee] = {};
    TYPE_COLS.forEach(t => { byYear[row.Annee][t] = (byYear[row.Annee][t]||0) + (row[t]||0); });
  });
  const years = Object.keys(byYear).map(Number).sort((a,b) => a - b).map(String);

  content.innerHTML = `
  <div class="section-header">
    <h2>Nombre de logements par type de construction résidentielle, ${annee}</h2>
    <div class="meta"><span class="mrc-tag">${mrcLabel}</span> Rôle d'évaluation foncière ${annee}</div>
  </div>

  <div class="charts-grid wide" style="margin-bottom:24px">
    <div class="chart-card">
      <h3>Évolution ${years.length ? `${years[0]}–${years[years.length - 1]}` : annee}</h3>
      <div class="chart-subtitle">Nombre de logements par type de construction</div>
      <div class="chart-legend" id="legend-log"></div>
      <div class="chart-area" id="chart-stacked"></div>
    </div>
  </div>

  <div class="charts-grid">
    <div class="chart-card">
      <h3>Répartition par type, ${annee}</h3>
      <div class="chart-subtitle">MRC : ${mrcLabel} · Total : ${total.toLocaleString('fr-CA')}</div>
      <div class="chart-area" id="chart-bar-types"></div>
    </div>
    <div class="chart-card" style="overflow-x:auto">
      <h3>Données détaillées ${annee}</h3>
      <div class="chart-subtitle">Par type de construction résidentielle</div>
      <div id="table-types"></div>
    </div>
  </div>

  ${munData.length > 0 ? `
  <div class="charts-grid wide" style="margin-top:24px">
    <div class="chart-card" style="overflow-x:auto">
      <h3>Par municipalité, ${annee}</h3>
      <div class="chart-subtitle">Nombre de logements par type de construction résidentielle</div>
      <div id="table-mun"></div>
    </div>
  </div>` : ''}

  <div class="footnote">${FOOTNOTE}<br>Source : Ministère des Affaires municipales et de l'Habitation (MAMH), rôle d'évaluation foncière des municipalités.</div>
  `;

  // Legend
  const legendEl = document.getElementById('legend-log');
  TYPE_COLS.forEach(t => {
    legendEl.innerHTML += `<div class="legend-item"><div class="legend-dot" style="background:${TYPE_COLORS[t]}"></div>${t}</div>`;
  });

  // Stacked bar chart (multi-year)
  drawStackedBar('chart-stacked', years, byYear, TYPE_COLS);

  // Simple bar chart (single year)
  drawHorizBar('chart-bar-types', TYPE_COLS, agg, total);

  // Table MRC
  const tableEl = document.getElementById('table-types');
  const yearHeaders = years.map(y => `<th class="num">${y}</th>`).join('');
  let html = `<table class="data-table"><thead><tr><th>Type</th>${yearHeaders}<th class="num">%</th></tr></thead><tbody>`;
  TYPE_COLS.forEach(t => {
    const vals = years.map(y => (byYear[y]?.[t]||0));
    const pct = total > 0 ? ((agg[t]||0)/total*100).toFixed(1) : '0.0';
    html += `<tr><td>${t}</td>${vals.map(v=>`<td class="num">${v.toLocaleString('fr-CA')}</td>`).join('')}<td class="pct">${pct}%</td></tr>`;
  });
  // Total row
  const totals = years.map(y => TYPE_COLS.reduce((s,t)=>s+(byYear[y]?.[t]||0),0));
  html += `<tr class="total-row"><td>Total</td>${totals.map(v=>`<td class="num">${v.toLocaleString('fr-CA')}</td>`).join('')}<td class="pct">100%</td></tr>`;
  html += '</tbody></table>';
  tableEl.innerHTML = html;

  // Table MUN
  if (munData.length > 0) {
    const munEl = document.getElementById('table-mun');
    let mhtml = `<table class="data-table"><thead><tr><th>Année</th><th>Municipalité</th><th>Type de construction résidentielle</th><th class="num">Nombre de logements</th></tr></thead><tbody>`;
    munData.slice(0, 200).forEach(d => {
      mhtml += `<tr><td>${d.Annee}</td><td>${d.CSDNAME}</td><td>${d['Types de construction résidentielle']}</td><td class="num">${(d['Nombre de logements']||0).toLocaleString('fr-CA')}</td></tr>`;
    });
    mhtml += '</tbody></table>';
    munEl.innerHTML = mhtml;
  }
}

// ── TAB: VALEUR ───────────────────────────────────────────────────────────────
function renderValeur(content, mrcLabel, annee, filterYearMRC, filterYearRangeMRC) {
  const mrcData = filterYearMRC(DATA.valeur_mrc || []);
  const munData = filterYearMRC(DATA.valeur_mun || []);

  // Aggregate
  const byType = {};
  mrcData.forEach(d => {
    if (!byType[d.Types]) byType[d.Types] = {terrain:0,batiment:0,immeuble:0,n:0};
    byType[d.Types].terrain  += d.terrain  || 0;
    byType[d.Types].batiment += d.batiment || 0;
    byType[d.Types].immeuble += d.immeuble || 0;
    byType[d.Types].n++;
  });
  // Average
  Object.keys(byType).forEach(t => {
    const n = byType[t].n;
    byType[t].terrain  /= n;
    byType[t].batiment /= n;
    byType[t].immeuble /= n;
  });

  // Multi-year
  const allVals = (DATA.valeur_mrc||[]).filter(d => d.CDNAME === (currentMRC||d.CDNAME) && d.Types === "Total des unités d'évaluation résidentielles");
  const byYear = {};
  allVals.forEach(d => { if(!byYear[d.Annee]) byYear[d.Annee]={terrain:0,batiment:0,n:0}; byYear[d.Annee].terrain+=d.terrain||0; byYear[d.Annee].batiment+=d.batiment||0; byYear[d.Annee].n++; });
  Object.keys(byYear).forEach(y => { byYear[y].terrain/=byYear[y].n; byYear[y].batiment/=byYear[y].n; });

  const types = Object.keys(byType).filter(t => t !== "Total des unités d'évaluation résidentielles");
  const total = byType["Total des unités d'évaluation résidentielles"];

  content.innerHTML = `
  <div class="section-header">
    <h2>Valeur foncière moyenne des propriétés, ${annee}</h2>
    <div class="meta"><span class="mrc-tag">${mrcLabel}</span> Valeurs moyennes en dollars</div>
  </div>

  <div class="charts-grid wide" style="margin-bottom:24px">
    <div class="chart-card">
      <h3>Valeur foncière moyenne par type, ${annee}</h3>
      <div class="chart-subtitle">Valeur du terrain et du bâtiment · MRC : ${mrcLabel}</div>
      <div class="chart-legend"><div class="legend-item"><div class="legend-dot" style="background:#1a4e4b"></div>Valeur du terrain</div><div class="legend-item"><div class="legend-dot" style="background:#6dbfb8"></div>Valeur du bâtiment</div></div>
      <div class="chart-area" id="chart-val-types"></div>
    </div>
  </div>

  <div class="charts-grid">
    <div class="chart-card" style="overflow-x:auto">
      <h3>Tableau détaillé ${annee}</h3>
      <div id="table-val"></div>
    </div>
    <div class="chart-card" style="overflow-x:auto">
      <h3>Par municipalité ${annee}</h3>
      <div id="table-val-mun"></div>
    </div>
  </div>

  <div class="footnote">${FOOTNOTE}<br>Source : Ministère des affaires municipales et habitation.</div>
  `;

  // Bullet/grouped bar
  drawValeurChart('chart-val-types', types, byType);

  // Table
  const tableEl = document.getElementById('table-val');
  // Multi-year for total row
  const allData = filterYearRangeMRC(DATA.valeur_mrc || []);
  const byYearType = {};
  allData.forEach(d => { if(!byYearType[d.Types]) byYearType[d.Types]={}; if(!byYearType[d.Types][d.Annee]) byYearType[d.Types][d.Annee]={t:0,b:0,i:0,n:0}; byYearType[d.Types][d.Annee].t+=d.terrain||0; byYearType[d.Types][d.Annee].b+=d.batiment||0; byYearType[d.Types][d.Annee].i+=d.immeuble||0; byYearType[d.Types][d.Annee].n++; });
  const years = Object.keys(byYearType).map(Number).sort((a,b) => a - b).map(String);
  let html = `<table class="data-table"><thead><tr><th>Année</th><th>Type</th><th class="num">Terrain</th><th class="num">Bâtiment</th><th class="num">Immeuble</th></tr></thead><tbody>`;
  years.forEach(y => {
    [...types, "Total des unités d'évaluation résidentielles"].forEach(t => {
      const d = byYearType[t]?.[y];
      if (!d) return;
      const n = d.n||1;
      const isTotal = t.startsWith('Total');
      html += `<tr${isTotal?' class="total-row"':''}><td>${y}</td><td>${t}</td><td class="num">${Math.round(d.t/n).toLocaleString('fr-CA')} $</td><td class="num">${Math.round(d.b/n).toLocaleString('fr-CA')} $</td><td class="num">${Math.round(d.i/n).toLocaleString('fr-CA')} $</td></tr>`;
    });
  });
  html += '</tbody></table>';
  tableEl.innerHTML = html;

  // Mun table
  const munEl = document.getElementById('table-val-mun');
  let mhtml = `<table class="data-table"><thead><tr><th>Municipalité</th><th>Type</th><th class="num">Terrain</th><th class="num">Bâtiment</th><th class="num">Immeuble</th></tr></thead><tbody>`;
  (munData||[]).slice(0,100).forEach(d => {
    mhtml += `<tr><td>${d.CSDNAME}</td><td>${d.Types}</td><td class="num">${Math.round(d.terrain||0).toLocaleString('fr-CA')} $</td><td class="num">${Math.round(d.batiment||0).toLocaleString('fr-CA')} $</td><td class="num">${Math.round(d.immeuble||0).toLocaleString('fr-CA')} $</td></tr>`;
  });
  mhtml += '</tbody></table>';
  munEl.innerHTML = mhtml;
}

// ── TAB: AGE ──────────────────────────────────────────────────────────────────
function renderAge(content, mrcLabel, annee, filterYearMRC, filterYearRangeMRC) {
  const mrcData = filterYearMRC(DATA.age_mrc || []);
  const munData = filterYearMRC(DATA.age_mun || []);

  const byType = {};
  mrcData.forEach(d => { if(!byType[d.Types]) byType[d.Types]={age:0,n:0}; byType[d.Types].age+=d.age_moyen||0; byType[d.Types].n++; });
  Object.keys(byType).forEach(t => { byType[t].age /= byType[t].n; });

  const sortedTypes = Object.entries(byType)
    .filter(([t]) => t !== "Total des unités d'évaluation résidentielles")
    .sort(([,a],[,b]) => a.age - b.age);
  const total = byType["Total des unités d'évaluation résidentielles"];

  content.innerHTML = `
  <div class="section-header">
    <h2>L'âge moyen des bâtiments selon les types de construction, ${annee}</h2>
    <div class="meta"><span class="mrc-tag">${mrcLabel}</span> En années · Rôle d'évaluation foncière ${annee}</div>
  </div>

  <div class="charts-grid wide" style="margin-bottom:24px">
    <div class="chart-card">
      <h3>Âge moyen par type de construction, ${annee}</h3>
      <div class="chart-subtitle">MRC : ${mrcLabel} · Total des unités : ${total ? Math.round(total.age)+' ans' : 'N/D'}</div>
      <div class="chart-area" id="chart-age"></div>
    </div>
  </div>

  <div class="charts-grid wide">
    <div class="chart-card" style="overflow-x:auto">
      <h3>Par municipalité, ${annee}</h3>
      <div id="table-age-mun"></div>
    </div>
  </div>

  <div class="footnote">${FOOTNOTE}<br>Source : Ministère des Affaires municipales et de l'Habitation (MAMH), rôle d'évaluation foncière des municipalités.</div>
  `;

  drawAgeChart('chart-age', sortedTypes, total);

  // Mun table
  const munEl = document.getElementById('table-age-mun');
  let mhtml = `<table class="data-table"><thead><tr><th>Municipalité</th><th>Types de construction résidentielle</th><th class="num">Âge moyen des logements</th></tr></thead><tbody>`;
  (munData||[]).slice(0,200).forEach(d => {
    const isTotal = (d.Types||'').startsWith('Total');
    mhtml += `<tr${isTotal?' class="total-row"':''}><td>${d.CSDNAME}</td><td>${d.Types}</td><td class="num">${Math.round(d.age_moyen||0)}</td></tr>`;
  });
  mhtml += '</tbody></table>';
  munEl.innerHTML = mhtml;
}

// ─── CHART FUNCTIONS ──────────────────────────────────────────────────────────
function drawStackedBar(id, years, byYear, typeCols) {
  const el = document.getElementById(id);
  if (!el || !years.length) return;
  const W = el.clientWidth || 700, H = 240;
  const margin = {top:10, right:80, bottom:30, left:60};
  const w = W - margin.left - margin.right;
  const h = H - margin.top - margin.bottom;

  const svg = d3.select(`#${id}`).append('svg')
    .attr('width','100%').attr('viewBox',`0 0 ${W} ${H}`);
  const g = svg.append('g').attr('transform',`translate(${margin.left},${margin.top})`);

  const stack = d3.stack().keys(typeCols)(years.map(y => ({year:y, ...byYear[y]})));
  const maxY = d3.max(stack[stack.length-1], d => d[1]);

  const x = d3.scaleBand().domain(years).range([0,w]).padding(0.25);
  const y = d3.scaleLinear().domain([0, maxY*1.05]).range([h,0]);

  g.append('g').attr('class','grid').call(d3.axisLeft(y).tickSize(-w).tickFormat(''));
  g.append('g').attr('transform',`translate(0,${h})`).call(d3.axisBottom(x).tickSize(0)).select('.domain').remove();
  g.append('g').call(d3.axisLeft(y).ticks(5).tickFormat(d=>d.toLocaleString('fr-CA'))).select('.domain').remove();

  typeCols.forEach((type, i) => {
    const layer = stack[i];
    g.selectAll(`.bar-${i}`).data(layer).join('rect')
      .attr('class',`bar bar-${i}`)
      .attr('x', d => x(d.data.year))
      .attr('y', d => y(d[1]))
      .attr('height', d => Math.max(0, y(d[0]) - y(d[1])))
      .attr('width', x.bandwidth())
      .attr('fill', TYPE_COLORS[type])
      .on('mouseover', (event, d) => showTooltip(event,
        `<strong>${d.data.year}</strong><br>${type}<br>${(d[1]-d[0]).toLocaleString('fr-CA')} logements`))
      .on('mousemove', moveTooltip)
      .on('mouseout', hideTooltip);
  });

  // Totals on top
  years.forEach(y_val => {
    const tot = typeCols.reduce((s,t) => s+(byYear[y_val]?.[t]||0), 0);
    g.append('text').attr('class','total-label')
      .attr('x', x(y_val)+x.bandwidth()/2).attr('y', y(tot)-5)
      .attr('text-anchor','middle').attr('font-size','11px').attr('fill','#1a1a1a')
      .text(tot.toLocaleString('fr-CA'));
  });
}

function drawHorizBar(id, types, agg, total) {
  const el = document.getElementById(id);
  if (!el) return;
  const W = el.clientWidth || 400, H = Math.max(types.length*36+40, 200);
  const margin = {top:10, right:80, bottom:20, left:240};
  const w = W - margin.left - margin.right;
  const h = H - margin.top - margin.bottom;

  const svg = d3.select(`#${id}`).append('svg')
    .attr('width','100%').attr('viewBox',`0 0 ${W} ${H}`);
  const g = svg.append('g').attr('transform',`translate(${margin.left},${margin.top})`);

  const sorted = [...types].sort((a,b) => (agg[a]||0)-(agg[b]||0));
  const x = d3.scaleLinear().domain([0, d3.max(types, t=>agg[t]||0)*1.15]).range([0,w]);
  const y = d3.scaleBand().domain(sorted).range([h,0]).padding(0.3);

  sorted.forEach(t => {
    const isTotal = t.startsWith('Total');
    g.append('rect').attr('class','bar')
      .attr('y', y(t)).attr('height', y.bandwidth())
      .attr('x', 0).attr('width', x(agg[t]||0))
      .attr('fill', isTotal ? '#3d8f8a' : TYPE_COLORS[t])
      .on('mouseover', (event) => showTooltip(event,
        `${t}<br><strong>${(agg[t]||0).toLocaleString('fr-CA')}</strong> logements<br>${total>0?((agg[t]||0)/total*100).toFixed(1):'0'}%`))
      .on('mousemove', moveTooltip).on('mouseout', hideTooltip);

    g.append('text').attr('class','bar-label')
      .attr('x', x(agg[t]||0)+6).attr('y', y(t)+y.bandwidth()/2+4)
      .attr('font-size','11px').attr('fill','#1a1a1a')
      .text((agg[t]||0).toLocaleString('fr-CA'));

    g.append('text').attr('class','bar-label')
      .attr('x', -8).attr('y', y(t)+y.bandwidth()/2+4)
      .attr('text-anchor','end').attr('font-size','11px').attr('fill','#5a5a5a')
      .text(t.length > 35 ? t.substring(0,33)+'…' : t);
  });
}

function drawValeurChart(id, types, byType) {
  const el = document.getElementById(id);
  if (!el || !types.length) return;
  const W = el.clientWidth || 700, H = types.length*52+60;
  const margin = {top:10, right:120, bottom:20, left:250};
  const w = W - margin.left - margin.right;
  const h = H - margin.top - margin.bottom;

  const svg = d3.select(`#${id}`).append('svg')
    .attr('width','100%').attr('viewBox',`0 0 ${W} ${H}`);
  const g = svg.append('g').attr('transform',`translate(${margin.left},${margin.top})`);

  const sorted = [...types].sort((a,b) => (byType[a]?.immeuble||0)-(byType[b]?.immeuble||0));
  const maxVal = d3.max(types, t => byType[t]?.immeuble||0);
  const x = d3.scaleLinear().domain([0, maxVal*1.1]).range([0,w]);
  const y = d3.scaleBand().domain(sorted).range([h,0]).padding(0.35);
  const barH = y.bandwidth();

  sorted.forEach(t => {
    const d = byType[t] || {};
    // Terrain bar
    g.append('rect').attr('y',y(t)).attr('height',barH/2-1)
      .attr('x',0).attr('width',x(d.terrain||0)).attr('fill','#1a4e4b').attr('rx',2);
    // Bâtiment bar
    g.append('rect').attr('y',y(t)+barH/2+1).attr('height',barH/2-1)
      .attr('x',0).attr('width',x(d.batiment||0)).attr('fill','#6dbfb8').attr('rx',2);
    // Value label
    g.append('text').attr('x',x(d.immeuble||0)+6).attr('y',y(t)+barH/2+4)
      .attr('font-size','11px').attr('fill','#1a1a1a').attr('font-weight','600')
      .text(Math.round(d.immeuble||0).toLocaleString('fr-CA')+' $');
    // Type label
    g.append('text').attr('x',-8).attr('y',y(t)+barH/2+4)
      .attr('text-anchor','end').attr('font-size','11px').attr('fill','#5a5a5a')
      .text(t.length>32 ? t.substring(0,30)+'…' : t);
  });
}

function drawAgeChart(id, sortedTypes, total) {
  const el = document.getElementById(id);
  if (!el || !sortedTypes.length) return;
  const W = el.clientWidth || 700, H = (sortedTypes.length+1)*48+60;
  const margin = {top:10, right:60, bottom:20, left:240};
  const w = W - margin.left - margin.right;
  const h = H - margin.top - margin.bottom;

  const allTypes = [...sortedTypes, ['Total des unités d\'évaluation résidentielles', total||{age:0}]];
  const maxAge = d3.max(allTypes, ([,d]) => d.age||0);

  const svg = d3.select(`#${id}`).append('svg')
    .attr('width','100%').attr('viewBox',`0 0 ${W} ${H}`);
  const g = svg.append('g').attr('transform',`translate(${margin.left},${margin.top})`);

  const x = d3.scaleLinear().domain([0, maxAge*1.1]).range([0,w]);
  const y = d3.scaleBand().domain(allTypes.map(([t]) => t)).range([h,0]).padding(0.3);

  g.append('g').attr('class','grid').call(d3.axisBottom(x).tickSize(h).tickFormat('').ticks(5))
    .attr('transform','translate(0,0)').select('.domain').remove();
  g.append('g').attr('transform',`translate(0,${h})`).call(d3.axisBottom(x).ticks(5)).select('.domain').remove();

  allTypes.forEach(([type, data]) => {
    const age = Math.round(data.age||0);
    const isTotal = type.startsWith('Total');
    g.append('rect').attr('class','bar')
      .attr('y',y(type)).attr('height',y.bandwidth())
      .attr('x',0).attr('width',x(age))
      .attr('fill', isTotal ? '#3d8f8a' : '#1a4e4b').attr('rx',3)
      .on('mouseover', (event) => showTooltip(event,`${type}<br><strong>${age} ans</strong>`))
      .on('mousemove', moveTooltip).on('mouseout', hideTooltip);

    g.append('text').attr('class','bar-label')
      .attr('x',x(age)+6).attr('y',y(type)+y.bandwidth()/2+4)
      .attr('font-size','12px').attr('font-weight',isTotal?'600':'400')
      .attr('fill','#1a1a1a').text(age);

    g.append('text').attr('x',-8).attr('y',y(type)+y.bandwidth()/2+4)
      .attr('text-anchor','end').attr('font-size','11px').attr('fill', isTotal?'#1a4e4b':'#5a5a5a')
      .attr('font-weight', isTotal?'600':'400')
      .text(type.length>33 ? type.substring(0,31)+'…' : type);
  });
}

// ─── EVENTS ───────────────────────────────────────────────────────────────────
document.getElementById('sel-region').addEventListener('change', function() {
  populateMRCSelect(this.value);
  currentMRC = null;
  drawMap();
  render();
});

document.getElementById('sel-mrc').addEventListener('change', function() {
  currentMRC = this.value || null;
  drawMap();
  render();
});

document.getElementById('sel-annee').addEventListener('change', function() {
  currentYear = parseInt(this.value, 10);
  drawMap();
  render();
});

document.getElementById('sel-annee-min').addEventListener('change', function() {
  currentYearMin = parseInt(this.value, 10);
  syncYearRange('min');
  drawMap();
  render();
});

document.getElementById('sel-annee-max').addEventListener('change', function() {
  currentYearMax = parseInt(this.value, 10);
  syncYearRange('max');
  drawMap();
  render();
});

document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', function() {
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    this.classList.add('active');
    currentTab = this.dataset.tab;
    render();
  });
});

// ─── INIT ─────────────────────────────────────────────────────────────────────
(async () => {
  try {
    await loadAll();
    populateYearSelect();
    populateMRCSelect('');
    drawMap();
    render();
  } catch (e) {
    document.getElementById('content').innerHTML =
      `<div class="loading">⚠ Erreur de chargement des données. Assurez-vous que le pipeline a été exécuté.<br><small>${e.message}</small></div>`;
    console.error(e);
  }
})();