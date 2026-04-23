// ─────────────────────────────────────────────────────────────────────────────
// DATA STORE
// ─────────────────────────────────────────────────────────────────────────────
const DATA = {};

const TYPE_COLORS = {
  "Autres immeubles résidentiels": "#8fbc8f",
  "Chalet et maison de villégiature": "#b2deda",
  "Habitation en commun": "#6dbfb8",
  "Immeuble comportant deux logements ou plus": "#5a6fa0",
  "Maison mobile et roulotte": "#4a90a4",
  "Maisons individuelles détachées": "#1a4e4b",
  "Maisons jumelées ou en rangée": "#c8b89a",
  "Logements dans un immeuble comportant deux logements ou plus": "#5a6fa0",
  "Total des unités d'évaluation résidentielles": "#3d8f8a",
};

const PERIODES_ORDER = [
  "1960 ou avant",
  "1961-1980",
  "1981-2000",
  "2001-2015",
  "2016 et plus"
];

const FOOTNOTE =
  "Les catégories MAMH sont d'abord identifiées selon l'univers CUBF retenu, puis classées selon le lien physique et le nombre de logements. Dans le mode « MAMH + autres catégories », les catégories détaillées restantes sont attribuées à partir du CUBF pour les immeubles hors univers MAMH.";

const SOURCE =
  "Ministère des Affaires municipales et de l'Habitation (MAMH), rôle d'évaluation foncière des municipalités.";

const CATEGORY_MODES = ["mamh_strict", "mamh_optional", "mamh_plus_others"];
const MODE_BASE_FILES = [
  "logements_types_mrc", "logements_types_mun",
  "valeur_mrc", "valeur_mun",
  "age_mrc", "age_mun",
  "periode_mrc", "periode_mun",
  "superficie_mrc", "superficie_mun"
];

let currentMRC = null;
let currentTab = "logements";
let currentYear = null;
let currentYearMin = null;
let currentYearMax = null;
let currentCategoryMode = "mamh_plus_others";

function dataKey(base) {
  return `${base}_${currentCategoryMode}`;
}
function getData(base) {
  return DATA[dataKey(base)] || [];
}

// ─────────────────────────────────────────────────────────────────────────────
// LOAD
// ─────────────────────────────────────────────────────────────────────────────
async function loadAll() {
  const staticFiles = [
    "mrc_list",
    "nouveaux_logements_mrc",
    "nouveaux_logements_mun",
    "densite_pu_mrc",
    "densite_pu_mun",
    "types_nouveaux_mrc",
    "types_nouveaux_mun"
  ];

  const files = [...staticFiles];
  MODE_BASE_FILES.forEach(base => {
    CATEGORY_MODES.forEach(mode => files.push(`${base}_${mode}`));
  });

  await Promise.all(files.map(async f => {
    try {
      const r = await fetch(`./data/${f}.json`);
      DATA[f] = await r.json();
    } catch (e) {
      DATA[f] = [];
      console.warn(`Missing: ${f}.json`);
    }
  }));
}

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────
function getAvailableYears() {
  const years = new Set();
  [
    getData("logements_types_mrc"),
    getData("valeur_mrc"),
    getData("age_mrc"),
    getData("periode_mrc"),
    getData("superficie_mrc")
  ].forEach(arr => {
    (arr || []).forEach(d => {
      const y = Number(d?.Annee);
      if (Number.isFinite(y)) years.add(y);
    });
  });
  return [...years].sort((a, b) => a - b);
}

function populateYearSelect() {
  const ys = document.getElementById("sel-annee");
  const mn = document.getElementById("sel-annee-min");
  const mx = document.getElementById("sel-annee-max");
  const years = getAvailableYears();

  if (!years.length) {
    [ys, mn, mx].forEach(sel => {
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

  const desc = years.slice().reverse()
    .map(y => `<option value="${y}">${y}</option>`)
    .join("");
  const asc = years
    .map(y => `<option value="${y}">${y}</option>`)
    .join("");

  if (ys) { ys.innerHTML = desc; ys.value = String(currentYear); }
  if (mn) { mn.innerHTML = asc; mn.value = String(currentYearMin); }
  if (mx) { mx.innerHTML = asc; mx.value = String(currentYearMax); }
}

function getFilteredYears() {
  return getAvailableYears().filter(y => {
    if (currentYearMin !== null && y < currentYearMin) return false;
    if (currentYearMax !== null && y > currentYearMax) return false;
    return true;
  });
}

function syncYearRange(changed) {
  if (changed === "min" && currentYearMin > currentYearMax) currentYearMax = currentYearMin;
  if (changed === "max" && currentYearMax < currentYearMin) currentYearMin = currentYearMax;

  const filtered = getFilteredYears();
  if (filtered.length && !filtered.includes(currentYear)) {
    currentYear = filtered[filtered.length - 1];
  }
  populateYearSelect();
}

function populateMRCSelect(regionFilter) {
  const sel = document.getElementById("sel-mrc");
  const mrcList = DATA.mrc_list || [];

  const filtered = regionFilter
    ? mrcList.filter(d => String(d.Region) === regionFilter)
    : mrcList;

  sel.innerHTML =
    '<option value="">Toutes les MRC</option>' +
    filtered.map(d => `<option value="${d.CDNAME}">${d.CDNAME}</option>`).join("");

  if (currentMRC && filtered.find(d => d.CDNAME === currentMRC)) {
    sel.value = currentMRC;
  } else {
    currentMRC = null;
  }
}

function showTooltip(event, html) {
  const t = document.getElementById("tooltip");
  if (!t) return;
  t.innerHTML = html;
  t.style.opacity = "1";
  moveTooltip(event);
}
function moveTooltip(event) {
  const t = document.getElementById("tooltip");
  if (!t) return;
  t.style.left = (event.clientX + 12) + "px";
  t.style.top = (event.clientY - 10) + "px";
}
function hideTooltip() {
  const t = document.getElementById("tooltip");
  if (!t) return;
  t.style.opacity = "0";
}

function chartMeta({ note = "", source = "" }) {
  return `
    <div class="footnote">
      ${note ? `${note}<br>` : ""}
      Source : ${source}
    </div>
  `;
}

function clearChart(id) {
  const el = document.getElementById(id);
  if (el) el.innerHTML = "";
}

function uniqueTypesFromRows(rows, field = "Types") {
  return [...new Set((rows || []).map(d => d[field]).filter(Boolean))];
}

function filterHelpers() {
  const annee = currentYear;
  const filterMRC = arr => currentMRC ? arr.filter(d => d.CDNAME === currentMRC) : arr;
  const filterYear = arr => arr.filter(d => Number(d.Annee) === Number(annee));
  const filterYearRange = arr => arr.filter(d => {
    const y = Number(d.Annee);
    if (currentYearMin !== null && y < currentYearMin) return false;
    if (currentYearMax !== null && y > currentYearMax) return false;
    return true;
  });
  return {
    filterMRC,
    filterYear,
    filterYearRange,
    filterYearMRC: arr => filterMRC(filterYear(arr)),
    filterYearRangeMRC: arr => filterMRC(filterYearRange(arr))
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// CHARTS
// ─────────────────────────────────────────────────────────────────────────────
function drawStackedBar(id, years, byYear, typeCols) {
  const el = document.getElementById(id);
  if (!el || !years.length) return;

  const W = el.clientWidth || 900;
  const H = 280;
  const margin = { top: 16, right: 20, bottom: 40, left: 60 };
  const w = W - margin.left - margin.right;
  const h = H - margin.top - margin.bottom;

  const svg = d3.select(`#${id}`).append("svg")
    .attr("width", "100%")
    .attr("viewBox", `0 0 ${W} ${H}`);

  const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  const stackInput = years.map(y => ({ year: y, ...(byYear[y] || {}) }));
  const stack = d3.stack().keys(typeCols)(stackInput);
  const maxY = d3.max(stack[stack.length - 1] || [], d => d[1]) || 1;

  const x = d3.scaleBand().domain(years).range([0, w]).padding(0.25);
  const y = d3.scaleLinear().domain([0, maxY * 1.05]).range([h, 0]);

  g.append("g").attr("class", "grid")
    .call(d3.axisLeft(y).tickSize(-w).tickFormat(""))
    .select(".domain").remove();

  g.append("g")
    .attr("transform", `translate(0,${h})`)
    .call(d3.axisBottom(x).tickSize(0))
    .select(".domain").remove();

  g.append("g")
    .call(d3.axisLeft(y).ticks(5).tickFormat(d => d.toLocaleString("fr-CA")))
    .select(".domain").remove();

  typeCols.forEach((type, i) => {
    g.selectAll(`.bar-${i}`)
      .data(stack[i] || [])
      .join("rect")
      .attr("class", `bar bar-${i}`)
      .attr("x", d => x(d.data.year))
      .attr("y", d => y(d[1]))
      .attr("height", d => Math.max(0, y(d[0]) - y(d[1])))
      .attr("width", x.bandwidth())
      .attr("fill", TYPE_COLORS[type] || "#6dbfb8")
      .on("mouseover", (event, d) => showTooltip(
        event,
        `<strong>${d.data.year}</strong><br>${type}<br>${(d[1] - d[0]).toLocaleString("fr-CA")} logements`
      ))
      .on("mousemove", moveTooltip)
      .on("mouseout", hideTooltip);
  });
}

function drawHorizBar(id, types, agg) {
  const el = document.getElementById(id);
  if (!el || !types.length) return;

  const sorted = [...types].sort((a, b) => (agg[a] || 0) - (agg[b] || 0));

  const W = el.clientWidth || 700;
  const H = Math.max(sorted.length * 36 + 30, 220);
  const margin = { top: 10, right: 90, bottom: 20, left: 260 };
  const w = W - margin.left - margin.right;
  const h = H - margin.top - margin.bottom;

  const svg = d3.select(`#${id}`).append("svg")
    .attr("width", "100%")
    .attr("viewBox", `0 0 ${W} ${H}`);

  const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  const maxVal = d3.max(sorted, t => agg[t] || 0) || 1;
  const x = d3.scaleLinear().domain([0, maxVal * 1.15]).range([0, w]);
  const y = d3.scaleBand().domain(sorted).range([0, h]).padding(0.25);

  sorted.forEach(t => {
    const v = agg[t] || 0;
    g.append("rect")
      .attr("class", "bar")
      .attr("x", 0)
      .attr("y", y(t))
      .attr("width", x(v))
      .attr("height", y.bandwidth())
      .attr("fill", TYPE_COLORS[t] || "#6dbfb8")
      .on("mouseover", event => showTooltip(event, `${t}<br><strong>${v.toLocaleString("fr-CA")}</strong>`))
      .on("mousemove", moveTooltip)
      .on("mouseout", hideTooltip);

    g.append("text")
      .attr("x", -10)
      .attr("y", y(t) + y.bandwidth() / 2 + 4)
      .attr("text-anchor", "end")
      .attr("font-size", "11px")
      .text(t.length > 40 ? t.slice(0, 38) + "…" : t);

    g.append("text")
      .attr("x", x(v) + 6)
      .attr("y", y(t) + y.bandwidth() / 2 + 4)
      .attr("font-size", "11px")
      .text(v.toLocaleString("fr-CA"));
  });
}

function drawPeriodeChart(id, totals) {
  const el = document.getElementById(id);
  if (!el) return;

  const data = PERIODES_ORDER.map(p => ({ label: p, value: totals[p] || 0 }));

  const W = el.clientWidth || 900;
  const H = 320;
  const margin = { top: 16, right: 20, bottom: 60, left: 70 };
  const w = W - margin.left - margin.right;
  const h = H - margin.top - margin.bottom;

  const svg = d3.select(`#${id}`).append("svg")
    .attr("width", "100%")
    .attr("viewBox", `0 0 ${W} ${H}`);

  const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  const maxVal = d3.max(data, d => d.value) || 1;
  const x = d3.scaleBand().domain(data.map(d => d.label)).range([0, w]).padding(0.3);
  const y = d3.scaleLinear().domain([0, maxVal * 1.1]).range([h, 0]);

  g.append("g").attr("class", "grid")
    .call(d3.axisLeft(y).tickSize(-w).tickFormat(""))
    .select(".domain").remove();

  g.append("g")
    .call(d3.axisLeft(y).ticks(5).tickFormat(d => d.toLocaleString("fr-CA")))
    .select(".domain").remove();

  g.append("g")
    .attr("transform", `translate(0,${h})`)
    .call(d3.axisBottom(x))
    .select(".domain").remove();

  data.forEach(d => {
    g.append("rect")
      .attr("x", x(d.label))
      .attr("y", y(d.value))
      .attr("width", x.bandwidth())
      .attr("height", h - y(d.value))
      .attr("fill", "#55aba7")
      .on("mouseover", event => showTooltip(event, `<strong>${d.label}</strong><br>${d.value.toLocaleString("fr-CA")}`))
      .on("mousemove", moveTooltip)
      .on("mouseout", hideTooltip);

    g.append("text")
      .attr("x", x(d.label) + x.bandwidth() / 2)
      .attr("y", y(d.value) - 8)
      .attr("text-anchor", "middle")
      .attr("font-size", "11px")
      .attr("font-weight", "700")
      .text(d.value.toLocaleString("fr-CA"));
  });
}

function drawValeurChart(id, types, byType) {
  const el = document.getElementById(id);
  if (!el || !types.length) return;

  const W = el.clientWidth || 900;
  const H = Math.max(types.length * 44 + 40, 260);
  const margin = { top: 10, right: 120, bottom: 20, left: 280 };
  const w = W - margin.left - margin.right;
  const h = H - margin.top - margin.bottom;

  const svg = d3.select(`#${id}`).append("svg")
    .attr("width", "100%")
    .attr("viewBox", `0 0 ${W} ${H}`);

  const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  const maxVal = d3.max(types, t => (byType[t]?.terrain || 0) + (byType[t]?.batiment || 0)) || 1;
  const x = d3.scaleLinear().domain([0, maxVal * 1.1]).range([0, w]);
  const y = d3.scaleBand().domain(types).range([0, h]).padding(0.24);

  types.forEach(t => {
    const d = byType[t] || {};
    const terrain = Number(d.terrain) || 0;
    const batiment = Number(d.batiment) || 0;

    g.append("rect")
      .attr("x", 0)
      .attr("y", y(t))
      .attr("width", x(terrain))
      .attr("height", y.bandwidth())
      .attr("fill", "#0b5d5e");

    g.append("rect")
      .attr("x", x(terrain))
      .attr("y", y(t))
      .attr("width", x(batiment))
      .attr("height", y.bandwidth())
      .attr("fill", "#4fa7a5");

    g.append("text")
      .attr("x", -10)
      .attr("y", y(t) + y.bandwidth() / 2 + 4)
      .attr("text-anchor", "end")
      .attr("font-size", "11px")
      .text(t.length > 42 ? t.slice(0, 40) + "…" : t);

    g.append("text")
      .attr("x", x(terrain + batiment) + 8)
      .attr("y", y(t) + y.bandwidth() / 2 + 4)
      .attr("font-size", "11px")
      .attr("font-weight", "700")
      .text(Math.round((d.immeuble || 0)).toLocaleString("fr-CA") + " $");
  });
}

function drawAgeChart(id, sortedTypes, total) {
  const el = document.getElementById(id);
  if (!el || !sortedTypes.length) return;

  const allTypes = [...sortedTypes, ["Total des unités d'évaluation résidentielles", total || { age: 0 }]];
  const W = el.clientWidth || 800;
  const H = Math.max(allTypes.length * 42 + 40, 260);
  const margin = { top: 10, right: 60, bottom: 20, left: 260 };
  const w = W - margin.left - margin.right;
  const h = H - margin.top - margin.bottom;

  const svg = d3.select(`#${id}`).append("svg")
    .attr("width", "100%")
    .attr("viewBox", `0 0 ${W} ${H}`);

  const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  const maxAge = d3.max(allTypes, ([, d]) => d.age || 0) || 1;
  const x = d3.scaleLinear().domain([0, maxAge * 1.1]).range([0, w]);
  const y = d3.scaleBand().domain(allTypes.map(([t]) => t)).range([0, h]).padding(0.25);

  allTypes.forEach(([type, data]) => {
    const age = Math.round(data.age || 0);
    const isTotal = type.startsWith("Total");

    g.append("rect")
      .attr("x", 0)
      .attr("y", y(type))
      .attr("width", x(age))
      .attr("height", y.bandwidth())
      .attr("rx", 3)
      .attr("fill", isTotal ? "#3d8f8a" : "#1a4e4b");

    g.append("text")
      .attr("x", -10)
      .attr("y", y(type) + y.bandwidth() / 2 + 4)
      .attr("text-anchor", "end")
      .attr("font-size", "11px")
      .text(type.length > 42 ? type.slice(0, 40) + "…" : type);

    g.append("text")
      .attr("x", x(age) + 8)
      .attr("y", y(type) + y.bandwidth() / 2 + 4)
      .attr("font-size", "11px")
      .attr("font-weight", isTotal ? "700" : "400")
      .text(age);
  });
}

function drawMultiLineChart(id, years, series, suffix = "") {
  const el = document.getElementById(id);
  if (!el || !years.length || !series.length) return;

  const cleanSeries = series.filter(s => s.values.some(v => v.value !== null && v.value !== undefined));
  if (!cleanSeries.length) return;

  const W = el.clientWidth || 900;
  const H = 320;
  const margin = { top: 20, right: 30, bottom: 40, left: 70 };
  const w = W - margin.left - margin.right;
  const h = H - margin.top - margin.bottom;

  const svg = d3.select(`#${id}`).append("svg")
    .attr("width", "100%")
    .attr("viewBox", `0 0 ${W} ${H}`);

  const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  const x = d3.scalePoint().domain(years).range([0, w]);
  const maxY = d3.max(cleanSeries.flatMap(s => s.values.map(v => v.value).filter(v => v !== null))) || 1;
  const y = d3.scaleLinear().domain([0, maxY * 1.1]).range([h, 0]);

  g.append("g").attr("class", "grid")
    .call(d3.axisLeft(y).tickSize(-w).tickFormat(""))
    .select(".domain").remove();

  g.append("g")
    .call(d3.axisLeft(y).ticks(5).tickFormat(d => d.toLocaleString("fr-CA")))
    .select(".domain").remove();

  g.append("g")
    .attr("transform", `translate(0,${h})`)
    .call(d3.axisBottom(x))
    .select(".domain").remove();

  const color = d3.scaleOrdinal()
    .domain(cleanSeries.map(s => s.name))
    .range(cleanSeries.map(s => TYPE_COLORS[s.name] || "#6dbfb8"));

  const line = d3.line()
    .defined(d => d.value !== null && d.value !== undefined)
    .x(d => x(d.year))
    .y(d => y(d.value));

  cleanSeries.forEach(s => {
    g.append("path")
      .datum(s.values)
      .attr("fill", "none")
      .attr("stroke", color(s.name))
      .attr("stroke-width", 2.5)
      .attr("d", line);

    g.selectAll(`.pt-${CSS.escape(s.name)}`)
      .data(s.values.filter(line.defined()))
      .join("circle")
      .attr("cx", d => x(d.year))
      .attr("cy", d => y(d.value))
      .attr("r", 3.5)
      .attr("fill", color(s.name))
      .on("mouseover", (event, d) => showTooltip(
        event,
        `<strong>${s.name}</strong><br>${d.year}<br>${Number(d.value).toLocaleString("fr-CA")}${suffix ? " " + suffix : ""}`
      ))
      .on("mousemove", moveTooltip)
      .on("mouseout", hideTooltip);
  });

  const legend = d3.select(`#${id}`).append("div")
    .attr("class", "chart-legend")
    .style("margin-top", "12px");

  cleanSeries.forEach(s => {
    legend.append("div")
      .attr("class", "legend-item")
      .html(`<div class="legend-dot" style="background:${color(s.name)}"></div>${s.name}`);
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// TABLES
// ─────────────────────────────────────────────────────────────────────────────
function renderSimpleTable(id, columns, rows) {
  const el = document.getElementById(id);
  if (!el) return;

  let html = `<table class="data-table"><thead><tr>`;
  columns.forEach(c => { html += `<th>${c.label}</th>`; });
  html += `</tr></thead><tbody>`;

  rows.forEach(r => {
    html += `<tr>`;
    columns.forEach(c => {
      const v = r[c.key];
      html += `<td class="${c.numeric ? "num" : ""}">${
        c.numeric ? Number(v || 0).toLocaleString("fr-CA") + (c.suffix || "") : (v ?? "")
      }</td>`;
    });
    html += `</tr>`;
  });

  html += `</tbody></table>`;
  el.innerHTML = html;
}

// ─────────────────────────────────────────────────────────────────────────────
// TABS
// ─────────────────────────────────────────────────────────────────────────────
function renderLogements(content) {
  const { filterYearMRC, filterYearRangeMRC } = filterHelpers();
  const mrcLabel = currentMRC || "Toutes les MRC";
  const annee = currentYear;

  const mrcData = filterYearMRC(getData("logements_types_mrc"));
  const munData = filterYearMRC(getData("logements_types_mun"));

  const typeCols = uniqueTypesFromRows(
    mrcData.flatMap(d => Object.keys(d).filter(k => TYPE_COLORS[k] && !k.endsWith("_pct")))
  ).filter(t => t !== "Total des unités d'évaluation résidentielles");

  const agg = {};
  typeCols.forEach(t => agg[t] = 0);
  mrcData.forEach(row => {
    typeCols.forEach(t => { agg[t] += Number(row[t] || 0); });
  });

  const allYears = filterYearRangeMRC(getData("logements_types_mrc"));
  const byYear = {};
  allYears.forEach(row => {
    if (!byYear[row.Annee]) byYear[row.Annee] = {};
    typeCols.forEach(t => {
      byYear[row.Annee][t] = (byYear[row.Annee][t] || 0) + Number(row[t] || 0);
    });
  });
  const years = Object.keys(byYear).map(Number).sort((a, b) => a - b).map(String);

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
        ${chartMeta({ note: FOOTNOTE, source: SOURCE })}
      </div>
    </div>

    <div class="charts-grid">
      <div class="chart-card">
        <h3>Répartition par type, ${annee}</h3>
        <div class="chart-area" id="chart-bar-types"></div>
      </div>
      <div class="chart-card" style="overflow-x:auto">
        <h3>Par municipalité, ${annee}</h3>
        <div id="table-logements-mun"></div>
      </div>
    </div>
  `;

  const legendEl = document.getElementById("legend-log");
  typeCols.forEach(t => {
    legendEl.innerHTML += `<div class="legend-item"><div class="legend-dot" style="background:${TYPE_COLORS[t] || "#6dbfb8"}"></div>${t}</div>`;
  });

  clearChart("chart-stacked");
  clearChart("chart-bar-types");
  drawStackedBar("chart-stacked", years, byYear, typeCols);
  drawHorizBar("chart-bar-types", typeCols, agg);

  renderSimpleTable("table-logements-mun", [
    { key: "Annee", label: "Année", numeric: true },
    { key: "CSDNAME", label: "Municipalité" },
    { key: "CDNAME", label: "MRC" },
    { key: "Types de construction résidentielle", label: "Type" },
    { key: "Nombre de logements", label: "Logements", numeric: true }
  ], munData);
}

function renderValeur(content) {
  const { filterYearMRC } = filterHelpers();
  const mrcLabel = currentMRC || "Toutes les MRC";
  const annee = currentYear;

  const mrcData = filterYearMRC(getData("valeur_mrc"));
  const munData = filterYearMRC(getData("valeur_mun"));

  const byType = {};
  mrcData.forEach(d => {
    byType[d.Types] = {
      terrain: Number(d.terrain) || 0,
      batiment: Number(d.batiment) || 0,
      immeuble: Number(d.immeuble) || 0
    };
  });
  const types = Object.keys(byType);

  content.innerHTML = `
    <div class="section-header">
      <h2>Valeur foncière moyenne des propriétés, ${annee}</h2>
      <div class="meta"><span class="mrc-tag">${mrcLabel}</span> Valeurs moyennes en dollars</div>
    </div>

    <div class="charts-grid wide" style="margin-bottom:24px">
      <div class="chart-card">
        <h3>Valeur foncière moyenne par type, ${annee}</h3>
        <div class="chart-legend">
          <div class="legend-item"><div class="legend-dot" style="background:#0b5d5e"></div>Valeur du terrain</div>
          <div class="legend-item"><div class="legend-dot" style="background:#4fa7a5"></div>Valeur du bâtiment</div>
        </div>
        <div class="chart-area" id="chart-valeurs"></div>
        ${chartMeta({ note: FOOTNOTE, source: SOURCE })}
      </div>
    </div>

    <div class="charts-grid">
      <div class="chart-card" style="overflow-x:auto">
        <h3>Données MRC</h3>
        <div id="table-valeurs-mrc"></div>
      </div>
      <div class="chart-card" style="overflow-x:auto">
        <h3>Par municipalité</h3>
        <div id="table-valeurs-mun"></div>
      </div>
    </div>
  `;

  clearChart("chart-valeurs");
  drawValeurChart("chart-valeurs", types, byType);

  renderSimpleTable("table-valeurs-mrc", [
    { key: "Annee", label: "Année", numeric: true },
    { key: "Types", label: "Type" },
    { key: "terrain", label: "Terrain", numeric: true, suffix: " $" },
    { key: "batiment", label: "Bâtiment", numeric: true, suffix: " $" },
    { key: "immeuble", label: "Immeuble", numeric: true, suffix: " $" }
  ], mrcData);

  renderSimpleTable("table-valeurs-mun", [
    { key: "Annee", label: "Année", numeric: true },
    { key: "CSDNAME", label: "Municipalité" },
    { key: "CDNAME", label: "MRC" },
    { key: "Types", label: "Type" },
    { key: "terrain", label: "Terrain", numeric: true, suffix: " $" },
    { key: "batiment", label: "Bâtiment", numeric: true, suffix: " $" },
    { key: "immeuble", label: "Immeuble", numeric: true, suffix: " $" }
  ], munData);
}

function renderAge(content) {
  const { filterYearMRC } = filterHelpers();
  const mrcLabel = currentMRC || "Toutes les MRC";
  const annee = currentYear;

  const mrcData = filterYearMRC(getData("age_mrc"));
  const munData = filterYearMRC(getData("age_mun"));

  const byType = {};
  mrcData.forEach(d => {
    if (!byType[d.Types]) byType[d.Types] = { age: 0, n: 0 };
    byType[d.Types].age += Number(d.age_moyen || 0);
    byType[d.Types].n += 1;
  });
  Object.keys(byType).forEach(t => byType[t].age /= byType[t].n || 1);

  const sortedTypes = Object.entries(byType)
    .filter(([t]) => !t.startsWith("Total"))
    .sort(([, a], [, b]) => a.age - b.age);

  const total = byType["Total des unités d'évaluation résidentielles"];

  content.innerHTML = `
    <div class="section-header">
      <h2>L'âge moyen des bâtiments selon les types de construction, ${annee}</h2>
      <div class="meta"><span class="mrc-tag">${mrcLabel}</span> En années</div>
    </div>

    <div class="charts-grid wide" style="margin-bottom:24px">
      <div class="chart-card">
        <h3>Âge moyen par type, ${annee}</h3>
        <div class="chart-area" id="chart-age"></div>
        ${chartMeta({ note: FOOTNOTE, source: SOURCE })}
      </div>
    </div>

    <div class="charts-grid wide">
      <div class="chart-card" style="overflow-x:auto">
        <h3>Par municipalité</h3>
        <div id="table-age-mun"></div>
      </div>
    </div>
  `;

  clearChart("chart-age");
  drawAgeChart("chart-age", sortedTypes, total);

  renderSimpleTable("table-age-mun", [
    { key: "Annee", label: "Année", numeric: true },
    { key: "CSDNAME", label: "Municipalité" },
    { key: "CDNAME", label: "MRC" },
    { key: "Types", label: "Type" },
    { key: "age_moyen", label: "Âge moyen", numeric: true }
  ], munData);
}

function renderPeriode(content) {
  const { filterYearMRC } = filterHelpers();
  const mrcLabel = currentMRC || "Toutes les MRC";
  const annee = currentYear;

  const mrcData = filterYearMRC(getData("periode_mrc"));
  const munData = filterYearMRC(getData("periode_mun"));

  const totals = Object.fromEntries(PERIODES_ORDER.map(p => [p, 0]));
  mrcData.forEach(d => {
    totals[d["Période"]] = (totals[d["Période"]] || 0) + Number(d.N || 0);
  });

  content.innerHTML = `
    <div class="section-header">
      <h2>Unité d'évaluation résidentielle selon la période de construction, ${annee}</h2>
      <div class="meta"><span class="mrc-tag">${mrcLabel}</span></div>
    </div>

    <div class="charts-grid wide" style="margin-bottom:24px">
      <div class="chart-card">
        <h3>Répartition par période de construction, ${annee}</h3>
        <div class="chart-area" id="chart-periode"></div>
        ${chartMeta({ source: SOURCE })}
      </div>
    </div>

    <div class="charts-grid wide">
      <div class="chart-card" style="overflow-x:auto">
        <h3>Par municipalité</h3>
        <div id="table-periode-mun"></div>
      </div>
    </div>
  `;

  clearChart("chart-periode");
  drawPeriodeChart("chart-periode", totals);

  renderSimpleTable("table-periode-mun", [
    { key: "Annee", label: "Année", numeric: true },
    { key: "CSDNAME", label: "Municipalité" },
    { key: "CDNAME", label: "MRC" },
    { key: "Types", label: "Type" },
    { key: "Période", label: "Période" },
    { key: "N", label: "Nombre", numeric: true }
  ], munData);
}

function renderSuperficie(content) {
  const { filterYearMRC, filterYearRangeMRC } = filterHelpers();
  const mrcLabel = currentMRC || "Toutes les MRC";
  const annee = currentYear;

  const mrcData = filterYearMRC(getData("superficie_mrc"));
  const munData = filterYearMRC(getData("superficie_mun"));
  const rangeData = filterYearRangeMRC(getData("superficie_mrc"));

  const years = [...new Set(rangeData.map(d => Number(d.Annee)).filter(Boolean))].sort((a, b) => a - b);
  const types = uniqueTypesFromRows(rangeData).filter(t => !t.startsWith("Total"));

  const terrainSeries = types.map(type => ({
    name: type,
    values: years.map(year => {
      const row = rangeData.find(d => Number(d.Annee) === year && d.Types === type);
      return { year, value: row ? Number(row.superficie_terrain || 0) : null };
    })
  }));

  const aireSeries = types.map(type => ({
    name: type,
    values: years.map(year => {
      const row = rangeData.find(d => Number(d.Annee) === year && d.Types === type);
      return { year, value: row ? Number(row.aire_etages || 0) : null };
    })
  }));

  content.innerHTML = `
    <div class="section-header">
      <h2>Superficie moyenne du terrain et aire d'étages</h2>
      <div class="meta"><span class="mrc-tag">${mrcLabel}</span> ${currentYearMin} à ${currentYearMax}</div>
    </div>

    <div class="charts-grid">
      <div class="chart-card">
        <h3>Superficie moyenne du terrain</h3>
        <div class="chart-area" id="chart-superficie-terrain"></div>
        ${chartMeta({ source: SOURCE })}
      </div>
      <div class="chart-card">
        <h3>Aire d'étages moyenne</h3>
        <div class="chart-area" id="chart-superficie-aire"></div>
        ${chartMeta({ source: SOURCE })}
      </div>
    </div>

    <div class="charts-grid" style="margin-top:24px">
      <div class="chart-card" style="overflow-x:auto">
        <h3>Données MRC, ${annee}</h3>
        <div id="table-superficie-mrc"></div>
      </div>
      <div class="chart-card" style="overflow-x:auto">
        <h3>Par municipalité, ${annee}</h3>
        <div id="table-superficie-mun"></div>
      </div>
    </div>
  `;

  clearChart("chart-superficie-terrain");
  clearChart("chart-superficie-aire");
  drawMultiLineChart("chart-superficie-terrain", years, terrainSeries, "m²");
  drawMultiLineChart("chart-superficie-aire", years, aireSeries, "m²");

  renderSimpleTable("table-superficie-mrc", [
    { key: "Annee", label: "Année", numeric: true },
    { key: "Types", label: "Type" },
    { key: "superficie_terrain", label: "Terrain", numeric: true },
    { key: "aire_etages", label: "Aire d'étages", numeric: true }
  ], mrcData);

  renderSimpleTable("table-superficie-mun", [
    { key: "Annee", label: "Année", numeric: true },
    { key: "CSDNAME", label: "Municipalité" },
    { key: "CDNAME", label: "MRC" },
    { key: "Types", label: "Type" },
    { key: "superficie_terrain", label: "Terrain", numeric: true },
    { key: "aire_etages", label: "Aire d'étages", numeric: true }
  ], munData);
}

function renderIndicateurs(content) {
  const mrcLabel = currentMRC || "Toutes les MRC";
  const annee = currentYear;

  const nouveaux = (DATA.nouveaux_logements_mrc || []).filter(d => !currentMRC || d.CDNAME === currentMRC);
  const densite = (DATA.densite_pu_mrc || []).filter(d => !currentMRC || d.CDNAME === currentMRC);
  const typesNouveaux = (DATA.types_nouveaux_mrc || []).filter(d => !currentMRC || d.CDNAME === currentMRC);

  const nouveauxYear = nouveaux.filter(d => Number(d.Annee_construction) === Number(annee));
  const densiteYear = densite.filter(d => Number(d.Annee_construction) === Number(annee));
  const totalNouveaux = nouveauxYear.reduce((s, d) => s + Number(d.logements_PU || 0), 0);
  const densiteVal = densiteYear.length ? Number(densiteYear[densiteYear.length - 1].densite_nette_PU || 0) : null;

  const byType = {};
  typesNouveaux
    .filter(d => Number(d.Annee_construction) === Number(annee))
    .forEach(d => { byType[d.Types] = (byType[d.Types] || 0) + Number(d.logements || 0); });

  const types = Object.keys(byType);

  content.innerHTML = `
    <div class="section-header">
      <h2>Indicateurs stratégiques, ${annee}</h2>
      <div class="meta"><span class="mrc-tag">${mrcLabel}</span> Périmètres d'urbanisation</div>
    </div>

    <div class="charts-grid">
      <div class="chart-card">
        <h3>Nouveaux logements</h3>
        <div style="font-size:2.2rem;font-weight:700;color:#1a4e4b">${totalNouveaux.toLocaleString("fr-CA")}</div>
        <div class="chart-subtitle">Logements localisés dans les PU</div>
      </div>
      <div class="chart-card">
        <h3>Densité nette PU</h3>
        <div style="font-size:2.2rem;font-weight:700;color:#1a4e4b">${densiteVal !== null ? densiteVal.toLocaleString("fr-CA") : "N/D"}</div>
        <div class="chart-subtitle">Logements par hectare</div>
      </div>
    </div>

    <div class="charts-grid wide" style="margin-top:24px">
      <div class="chart-card">
        <h3>Nouveaux logements par type</h3>
        <div class="chart-area" id="chart-indicateurs-types"></div>
        ${chartMeta({ source: SOURCE })}
      </div>
    </div>

    <div class="charts-grid" style="margin-top:24px">
      <div class="chart-card" style="overflow-x:auto">
        <h3>Nouveaux logements PU</h3>
        <div id="table-indic-nouveaux"></div>
      </div>
      <div class="chart-card" style="overflow-x:auto">
        <h3>Densité nette PU</h3>
        <div id="table-indic-densite"></div>
      </div>
    </div>
  `;

  clearChart("chart-indicateurs-types");
  drawHorizBar("chart-indicateurs-types", types, byType);

  renderSimpleTable("table-indic-nouveaux", [
    { key: "Annee_construction", label: "Année de construction", numeric: true },
    { key: "CDNAME", label: "MRC" },
    { key: "logements_PU", label: "Logements PU", numeric: true }
  ], nouveaux);

  renderSimpleTable("table-indic-densite", [
    { key: "Annee_construction", label: "Année de construction", numeric: true },
    { key: "CDNAME", label: "MRC" },
    { key: "densite_nette_PU", label: "Densité nette PU", numeric: true }
  ], densite);
}

// ─────────────────────────────────────────────────────────────────────────────
// EVENTS
// ─────────────────────────────────────────────────────────────────────────────
document.getElementById("sel-region")?.addEventListener("change", function () {
  populateMRCSelect(this.value);
  currentMRC = null;
  render();
});

document.getElementById("sel-mrc")?.addEventListener("change", function () {
  currentMRC = this.value || null;
  render();
});

document.getElementById("sel-annee")?.addEventListener("change", function () {
  currentYear = parseInt(this.value, 10);
  render();
});

document.getElementById("sel-annee-min")?.addEventListener("change", function () {
  currentYearMin = parseInt(this.value, 10);
  syncYearRange("min");
  render();
});

document.getElementById("sel-annee-max")?.addEventListener("change", function () {
  currentYearMax = parseInt(this.value, 10);
  syncYearRange("max");
  render();
});

// Optional: works only if you later add the selector to index.html
document.getElementById("sel-category-mode")?.addEventListener("change", function () {
  currentCategoryMode = this.value || "mamh_plus_others";
  populateYearSelect();
  render();
});

document.querySelectorAll(".tab-btn").forEach(btn => {
  btn.addEventListener("click", function () {
    document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
    this.classList.add("active");
    currentTab = this.dataset.tab;
    render();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// INIT
// ─────────────────────────────────────────────────────────────────────────────
(async () => {
  try {
    await loadAll();
    populateYearSelect();
    applyStateFromUrlSafe();
    populateMRCSelect(document.getElementById("sel-region")?.value || "");
    render();
  } catch (e) {
    document.getElementById("content").innerHTML =
      `<div class="loading">⚠ Erreur de chargement des données.<br><small>${e.message}</small></div>`;
    console.error(e);
  }
})();

function applyStateFromUrlSafe() {
  const params = new URLSearchParams(window.location.search);
  const tab = params.get("tab");
  if (tab) {
    currentTab = tab;
    document.querySelectorAll(".tab-btn").forEach(b => {
      b.classList.toggle("active", b.dataset.tab === currentTab);
    });
  }
  const mrc = params.get("mrc");
  if (mrc) currentMRC = mrc;
  const year = Number(params.get("year"));
  if (Number.isFinite(year) && year > 0) currentYear = year;
  const ymin = Number(params.get("ymin"));
  if (Number.isFinite(ymin) && ymin > 0) currentYearMin = ymin;
  const ymax = Number(params.get("ymax"));
  if (Number.isFinite(ymax) && ymax > 0) currentYearMax = ymax;
}
