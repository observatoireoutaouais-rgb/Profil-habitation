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
  "Total des unités d'évaluation résidentielles": "#3d8f8a",
};

const CATEGORY_MODES = ['mamh_strict','mamh_optional','mamh_plus_others'];
const MODE_BASE_FILES = [
  'logements_types_mrc','logements_types_mun',
  'valeur_mrc','valeur_mun',
  'age_mrc','age_mun',
  'periode_mrc','periode_mun',
  'superficie_mrc','superficie_mun'
];

let currentMRC = null;
let currentTab = 'logements';
let currentYear = null;
let currentCategoryMode = 'mamh_plus_others';

function dataKey(base){ return `${base}_${currentCategoryMode}`; }
function getData(base){ return DATA[dataKey(base)] || []; }

// ─────────────────────────────────────────────────────────────────────────────
// LOAD DATA
// ─────────────────────────────────────────────────────────────────────────────
async function loadAll() {
  const staticFiles = [
    'mrc_list',
    'nouveaux_logements_mrc',
    'densite_pu_mrc',
    'types_nouveaux_mrc'
  ];

  const files = [...staticFiles];
  MODE_BASE_FILES.forEach(base =>
    CATEGORY_MODES.forEach(mode => files.push(`${base}_${mode}`))
  );

  await Promise.all(files.map(async f => {
    try {
      const r = await fetch(`./data/${f}.json`);
      DATA[f] = await r.json();
    } catch {
      DATA[f] = [];
    }
  }));
}

// ─────────────────────────────────────────────────────────────────────────────
// YEAR
// ─────────────────────────────────────────────────────────────────────────────
function getAvailableYears() {
  const years = new Set();
  Object.values(DATA).forEach(arr => {
    (arr || []).forEach(d => {
      const y = Number(d.Annee);
      if (Number.isFinite(y)) years.add(y);
    });
  });
  return [...years].sort((a,b)=>a-b);
}

function populateYearSelect() {
  const sel = document.getElementById('sel-annee');
  const years = getAvailableYears();
  if (!years.length) return;

  currentYear = currentYear || years[years.length-1];
  sel.innerHTML = years.map(y=>`<option>${y}</option>`).join('');
  sel.value = currentYear;
}

// ─────────────────────────────────────────────────────────────────────────────
// MRC
// ─────────────────────────────────────────────────────────────────────────────
function populateMRCSelect(region) {
  const sel = document.getElementById('sel-mrc');
  const list = DATA.mrc_list || [];
  const filtered = region ? list.filter(d=>d.Region===region) : list;

  sel.innerHTML = '<option value="">Toutes les MRC</option>' +
    filtered.map(d=>`<option>${d.CDNAME}</option>`).join('');
}

// ─────────────────────────────────────────────────────────────────────────────
// RENDER SWITCH
// ─────────────────────────────────────────────────────────────────────────────
function render() {
  const content = document.getElementById('content');

  if (currentTab === 'logements') renderLogements(content);
  else if (currentTab === 'valeur') renderValeur(content);
  else if (currentTab === 'age') renderAge(content);
  else if (currentTab === 'periode') renderPeriode(content);
  else if (currentTab === 'superficie') renderSuperficie(content);
  else if (currentTab === 'indicateurs') renderIndicateurs(content);
}

// ─────────────────────────────────────────────────────────────────────────────
// LOGEMENTS
// ─────────────────────────────────────────────────────────────────────────────
function renderLogements(content){
  const data = getData('logements_types_mrc').filter(d=>d.Annee==currentYear);

  content.innerHTML = `<h2>Logements ${currentYear}</h2><pre>${JSON.stringify(data.slice(0,10),null,2)}</pre>`;
}

// ─────────────────────────────────────────────────────────────────────────────
// VALEUR
// ─────────────────────────────────────────────────────────────────────────────
function renderValeur(content){
  const data = getData('valeur_mrc').filter(d=>d.Annee==currentYear);

  content.innerHTML = `<h2>Valeur ${currentYear}</h2><pre>${JSON.stringify(data.slice(0,10),null,2)}</pre>`;
}

// ─────────────────────────────────────────────────────────────────────────────
// AGE
// ─────────────────────────────────────────────────────────────────────────────
function renderAge(content){
  const data = getData('age_mrc').filter(d=>d.Annee==currentYear);

  content.innerHTML = `<h2>Âge ${currentYear}</h2><pre>${JSON.stringify(data.slice(0,10),null,2)}</pre>`;
}

// ─────────────────────────────────────────────────────────────────────────────
// PERIODE
// ─────────────────────────────────────────────────────────────────────────────
function renderPeriode(content){
  const data = getData('periode_mrc').filter(d=>d.Annee==currentYear);

  content.innerHTML = `<h2>Période ${currentYear}</h2><pre>${JSON.stringify(data.slice(0,10),null,2)}</pre>`;
}

// ─────────────────────────────────────────────────────────────────────────────
// SUPERFICIE
// ─────────────────────────────────────────────────────────────────────────────
function renderSuperficie(content){
  const data = getData('superficie_mrc').filter(d=>d.Annee==currentYear);

  content.innerHTML = `<h2>Superficie ${currentYear}</h2><pre>${JSON.stringify(data.slice(0,10),null,2)}</pre>`;
}

// ─────────────────────────────────────────────────────────────────────────────
// INDICATEURS
// ─────────────────────────────────────────────────────────────────────────────
function renderIndicateurs(content){
  const data = DATA.nouveaux_logements_mrc || [];

  content.innerHTML = `<h2>Indicateurs</h2><pre>${JSON.stringify(data.slice(0,10),null,2)}</pre>`;
}

// ─────────────────────────────────────────────────────────────────────────────
// EVENTS
// ─────────────────────────────────────────────────────────────────────────────
document.getElementById('sel-category-mode').addEventListener('change', function() {
  currentCategoryMode = this.value;
  render();
});

document.getElementById('sel-annee').addEventListener('change', function() {
  currentYear = this.value;
  render();
});

document.getElementById('sel-region').addEventListener('change', function() {
  populateMRCSelect(this.value);
});

document.getElementById('sel-mrc').addEventListener('change', function() {
  currentMRC = this.value || null;
  render();
});

document.querySelectorAll('.tab-btn').forEach(btn=>{
  btn.addEventListener('click',()=>{
    document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));
    btn.classList.add('active');
    currentTab = btn.dataset.tab;
    render();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// INIT
// ─────────────────────────────────────────────────────────────────────────────
(async ()=>{
  await loadAll();
  populateYearSelect();
  populateMRCSelect('');
  render();
})();
