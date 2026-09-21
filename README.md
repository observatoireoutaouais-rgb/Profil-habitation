# Profil de l'habitation – Pipeline & Dashboard

Tableau de bord automatisé du rôle d'évaluation foncière pour **l'ensemble du
Québec** : les 1 128 municipalités de `MATCH.csv`, réparties dans 102 MRC et
territoires équivalents, sur les 17 régions administratives.

## Structure du repo

```
├── pipeline.py              ← Script de traitement (API XML MAMH + SHP provinciaux)
├── MATCH.csv                ← Correspondance Municipalité → MRC → Région (tout le Québec)
├── pf-mun-2023-2023.csv     ← Codes géographiques ↔ municipalités/MRC
├── pop-hist-mrc.xlsx        ← ISQ, population historique par MRC
├── pop-proj-mrc.xlsx        ← ISQ, projections de population par MRC
├── menages-proj-mrc.xlsx    ← ISQ, projections de ménages privés par MRC
├── web/
│   ├── index.html           ← Application web (dashboard, JS inclus)
│   ├── data/                ← JSONs générés par le pipeline + fichiers manuels
│   │   └── mun/             ← Détail municipal, un fichier par région administrative
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

## Couverture territoriale

`MATCH.csv` définit le périmètre du pipeline : toute municipalité qui y figure
est téléchargée, agrégée et proposée dans les sélecteurs du tableau de bord. Il
est construit à partir de `pf-mun-2023-2023.csv` (les 1 104 municipalités du
profil financier), auquel s'ajoutent 24 entrées qu'il ne couvre pas — territoires
non organisés et réserves de l'Outaouais, des Laurentides et de la Montérégie,
qui portent pourtant des données au rôle.

Deux règles de nommage s'appliquent, et **le pipeline les reproduit à
l'identique** (`normalize_mrc`, `load_pf_mun`) : les deux doivent rester alignés,
faute de quoi les MRC du rôle ne rejoindraient plus celles des données ISQ.

- Un territoire équivalent à une MRC est écrit `Hors MRC - X` au profil
  financier : le préfixe est retiré. Trois d'entre eux portent chez l'ISQ un
  autre nom (`Des Chenaux` → `Les Chenaux`, `Les Îles de la Madeleine` →
  `Communauté maritime des Îles-de-la-Madeleine`, `Nord du Québec` → `Jamésie`).
- Dix paires de municipalités partagent un nom à l'intérieur d'une même MRC
  (Bedford V et Bedford CT, Hatley M et Hatley CT…). Elles reçoivent le suffixe
  de désignation employé par le MAMH et l'ISQ, sans quoi leurs données
  fusionneraient. Partout ailleurs, c'est le **code géographique** — jamais le
  nom — qui identifie une municipalité : trente-deux noms sont portés par deux
  municipalités de MRC différentes.

### Ce qui ne couvre pas encore tout le Québec

| Donnée | Couverture |
|---|---|
| `taux_inoccupation_schl.json` | 10 MRC (fichier maintenu à la main) |
| `tenure_menages_mrc.json` | 18 MRC (fichier maintenu à la main) |
| Indicateurs PU (`nouveaux_logements_*`, `densite_pu_*`, `types_nouveaux_*`) | Dépend des `Role_*_PU.zip`, absents en CI |
| Territoires non organisés | Seuls ceux des régions 07, 15 et 16 sont listés |

Population, projections de population et projections de ménages (ISQ) couvrent
en revanche les 102 MRC.

## Sources et années couvertes

Le pipeline utilise le **rôle de l'année correspondante** :

- **2012 à 2022** : SHP provinciaux (`ROLE{YYYY}_SHP.zip`, téléchargés en CI)
- **2023 et plus** : API MAMH (index CSV + XML par municipalité)

Un fichier `web/data/qa_couverture.json` est généré à chaque exécution : il
liste, pour chaque année, le nombre de municipalités et d'unités d'évaluation
retenues, ainsi que les années absentes ou incomplètes (ex. : SHP rejeté par
le contrôle de qualité).

### Empreinte et durée d'exécution

À l'échelle du Québec, une année du rôle approche les quatre millions d'unités
d'évaluation. Le pipeline traite donc **une année à la fois** : elle est agrégée
puis libérée, et seuls les agrégats — de taille négligeable — sont empilés.
Comme tous les indicateurs exportés sont ventilés par année, le résultat est
identique à un traitement en bloc.

Deux conséquences à garder en tête :

- L'étape API télécharge un XML par municipalité et par année (≈ 1 100 × 4),
  contre moins de 250 auparavant. Prévoir plusieurs heures, et surveiller la
  limite de 6 h d'un job GitHub Actions.
- `build_indicateurs_pu` charge le CSV provincial des périmètres d'urbanisation
  en une fois et ne retient plus qu'une poignée de MRC : à exécuter sur une
  machine correctement dotée en mémoire.

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
| `regions.json` | Régions administratives présentes (alimente le sélecteur Région) |
| `qa_couverture.json` | Contrôle qualité : couverture par année |

### Où sont écrits les fichiers

Les indicateurs par MRC restent à la racine de `web/data/` : une centaine de MRC
tient dans quelques Mo, et le tableau de bord les charge pour le seul filtre CUBF
actif.

Les indicateurs **par municipalité** (`*_mun*`) sont écrits dans
`web/data/mun/`, éclatés par région administrative — `age_mun_mamh_strict_r7.json`,
`densite_pu_mun_r16.json`, etc. Le détail municipal du Québec entier pèse
plusieurs dizaines de Mo par indicateur ; la page ne télécharge que le fragment
de la région consultée. Tant qu'aucune région ni MRC n'est sélectionnée, aucun
fragment n'est chargé et les tableaux par municipalité restent masqués — le
sélecteur l'indique sous les contrôles. Le dossier est recréé à chaque exécution
du pipeline, afin qu'aucun fragment périmé ne subsiste.

`{filtre}` ∈ `mamh_strict`, `mamh_optional`, `mamh_plus_others` (voir la
modale « Méthodologie CUBF » du dashboard).

### Fichiers maintenus manuellement (non générés par le pipeline)

| Fichier | Contenu |
|---|---|
| `taux_inoccupation_schl.json` | Taux d'inoccupation locatif SCHL par MRC (2010–2023, MRC couvertes seulement) |
| `tenure_menages_mrc.json` | Mode d'occupation (propriétaire/locataire), Recensement 2021 |
