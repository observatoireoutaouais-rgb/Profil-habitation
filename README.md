# Profil de l'habitation – Pipeline & Dashboard

Tableau de bord automatisé du rôle d'évaluation foncière pour l'Outaouais, les Laurentides et la Montérégie.

## Structure du repo

```
├── pipeline.py              ← Script de traitement des XML MAMH
├── MATCH.csv                ← Correspondance Municipalité → MRC → Région
├── data/                    ← JSONs générés par le pipeline (auto)
├── maps/                    ← TopoJSON des 3 régions
├── web/
│   └── index.html           ← Application web (dashboard)
├── netlify.toml             ← Config Netlify
└── .github/workflows/
    └── update-data.yml      ← Automation GitHub Actions
```

## Setup

### 1. Cloner et configurer

```bash
git clone https://github.com/TON-USERNAME/TON-REPO.git
cd TON-REPO
pip install requests pandas numpy
```

### 2. Lancer le pipeline manuellement

```bash
python pipeline.py
```

Cela télécharge les données MAMH pour 2020–2026 et génère les fichiers dans `web/data/`.

### 3. Déployer sur Netlify

1. Aller sur [netlify.com](https://netlify.com)
2. "Add new site" → "Import an existing project"
3. Connecter votre repo GitHub
4. Publish directory: `web`
5. Build command: (laisser vide)
6. Deploy!

### 4. Automation GitHub Actions

Le pipeline se lance automatiquement chaque lundi à 6h UTC.
Pour lancer manuellement : GitHub → Actions → "Mise à jour des données" → "Run workflow"

## Années couvertes

Le pipeline utilise le **rôle de l'année correspondante** :
- Logements 2020 → Rôle 2020
- Logements 2021 → Rôle 2021
- Logements 2025 → Rôle 2025
- Logements 2026 → Rôle 2026
- etc.

## Données produites

| Fichier | Contenu |
|---|---|
| `logements_types_mrc.json` | Nb logements par type, MRC, année |
| `logements_types_mun.json` | Nb logements par type, municipalité, année |
| `valeur_mrc.json` | Valeur foncière moyenne par type, MRC, année |
| `valeur_mun.json` | Valeur foncière par type, municipalité, année |
| `age_mrc.json` | Âge moyen des bâtiments, MRC, année |
| `age_mun.json` | Âge moyen, municipalité, année |
| `periode_mrc.json` | Période de construction, MRC, année |
| `mrc_list.json` | Liste MRC → municipalités (pour le filtre UI) |
