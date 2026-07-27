# Profil de l'habitation – Pipeline & Dashboard

Tableau de bord automatisé du rôle d'évaluation foncière pour l'Outaouais, les Laurentides et la Montérégie.

## Structure du repo

```
├── pipeline.py              ← Script de traitement (API XML MAMH + SHP provinciaux)
├── MATCH.csv                ← Correspondance Municipalité → MRC → Région
├── pf-mun-2023-2023.csv     ← Codes géographiques ↔ municipalités/MRC
├── pop-hist-mrc.xlsx        ← ISQ, population historique par MRC
├── pop-proj-mrc.xlsx        ← ISQ, projections de population par MRC
├── menages-proj-mrc.xlsx    ← ISQ, projections de ménages privés par MRC
├── web/
│   ├── index.html           ← Application web (dashboard, JS inclus)
│   ├── data/                ← JSONs générés par le pipeline + fichiers manuels
│   └── maps/                ← TopoJSON des 3 régions
├── netlify.toml             ← Config Netlify (publie web/)
└── .github/workflows/
    └── main.yml             ← Automation GitHub Actions
```

## Setup

### 1. Cloner et configurer

```bash
git clone https://github.com/observatoireoutaouais-rgb/Profil-habitation.git
cd Profil-habitation
pip install requests pandas numpy openpyxl
```

### 2. Lancer le pipeline manuellement

```bash
python pipeline.py
```

Cela télécharge les données MAMH et génère les fichiers dans `web/data/`.

### 3. Déployer sur Netlify

1. Aller sur [netlify.com](https://netlify.com)
2. "Add new site" → "Import an existing project"
3. Connecter votre repo GitHub
4. Publish directory: `web`
5. Build command: (laisser vide)
6. Deploy!

### 4. Automation GitHub Actions

Le pipeline se lance automatiquement chaque lundi à 6h UTC (et à chaque push
sur `main` touchant `pipeline.py`, `MATCH.csv`, `pf-mun-2023-2023.csv` ou le
workflow). Pour lancer manuellement : GitHub → Actions → "Mise à jour des
données" → "Run workflow".

Le workflow télécharge les SHP historiques (2012–2022) avant d'exécuter le
pipeline. Les fichiers `Role_{YYYY}_PU.zip` (périmètres d'urbanisation) ne
sont **pas** disponibles en CI : les indicateurs PU (`nouveaux_logements_*`,
`densite_pu_*`, `types_nouveaux_*`) ne sont régénérés que lors d'une
exécution locale avec ces zips dans le répertoire du projet.

## Sources et années couvertes

Le pipeline utilise le **rôle de l'année correspondante** :

- **2012 à 2022** : SHP provinciaux (`ROLE{YYYY}_SHP.zip`, téléchargés en CI)
- **2023 et plus** : API MAMH (index CSV + XML par municipalité)

Un fichier `web/data/qa_couverture.json` est généré à chaque exécution : il
liste, pour chaque année, le nombre de municipalités et d'unités d'évaluation
retenues, ainsi que les années absentes ou incomplètes (ex. : SHP rejeté par
le contrôle de qualité).

## Données produites

| Fichier | Contenu |
|---|---|
| `logements_types_{mrc,mun}_{filtre}.json` | Nb logements par type, par MRC/municipalité et année |
| `valeur_{mrc,mun}_{filtre}.json` | Valeur foncière moyenne par type (+ `n_ue` pour pondération) |
| `age_{mrc,mun}_{filtre}.json` | Âge moyen des unités d'évaluation résidentielles (+ `n_ue` au niveau MRC) |
| `periode_{mrc,mun}_{filtre}.json` | Unités par période de construction |
| `superficie_{mrc,mun}_{filtre}.json` | Superficie terrain et aire d'étages moyennes (+ `n_ue` au niveau MRC) |
| `nouveaux_logements_{mrc,mun}.json` | Logements construits dans les PU (nécessite `Role_*_PU.zip`) |
| `types_nouveaux_{mrc,mun}.json` | Types des nouveaux logements dans les PU |
| `densite_pu_{mrc,mun}.json` | Densité résidentielle nette dans les PU (log/ha) |
| `population_mrc.json` | Population historique ISQ par MRC |
| `projections_pop_mrc.json` | Projections de population ISQ (3 scénarios) |
| `menages_proj_mrc.json` | Projections de ménages privés ISQ (3 scénarios) |
| `mrc_list.json` | Liste MRC → municipalités (pour le filtre UI) |
| `qa_couverture.json` | Contrôle qualité : couverture par année |

`{filtre}` ∈ `mamh_strict`, `mamh_optional`, `mamh_plus_others` (voir la
modale « Méthodologie CUBF » du dashboard).

### Fichiers maintenus manuellement (non générés par le pipeline)

| Fichier | Contenu |
|---|---|
| `taux_inoccupation_schl.json` | Taux d'inoccupation locatif SCHL par MRC (2010–2023, MRC couvertes seulement) |
| `tenure_menages_mrc.json` | Mode d'occupation (propriétaire/locataire), Recensement 2021 |
