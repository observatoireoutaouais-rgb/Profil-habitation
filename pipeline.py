"""
Rôle d'évaluation foncière – Pipeline XML API
Utilise le rôle de l'année correspondante pour chaque année
(rôle 2021 → logements 2021, rôle 2024 → logements 2024, etc.)
"""

import requests
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
import io, time, csv, json, os
from pathlib import Path

# ── Paramètres ────────────────────────────────────────────────────────────────

ANNEES = list(range(2012, 2027))

INDEX_URLS = {
    annee: f"https://donneesouvertes.affmunqc.net/role/indexRole{annee}.csv"
    for annee in ANNEES
}

MATCH_PATH = "MATCH.csv"
DATA_DIR   = Path("web/data")
DATA_DIR.mkdir(exist_ok=True)

TYPE_COLS = [
    "Autres immeubles résidentiels",
    "Chalet et maison de villégiature",
    "Habitation en commun",
    "Immeuble comportant deux logements ou plus",
    "Maison mobile et roulotte",
    "Maisons individuelles détachées",
    "Maisons jumelées ou en rangée",
]

# ── Fonctions utilitaires ──────────────────────────────────────────────────────

def load_match():
    df = pd.read_csv(MATCH_PATH)
    # Handle 2-col (no Region) or 3-col versions
    if len(df.columns) == 2:
        df.columns = ["Municipalité", "CDNAME"]
        df["Region"] = None
    else:
        df.columns = ["Municipalité", "CDNAME", "Region"]
    df["_mun_key"] = df["Municipalité"].str.strip()
    print(f"MATCH: {len(df)} municipalités, {df['CDNAME'].nunique()} MRC")
    return df


def fetch_index(annee):
    url = INDEX_URLS[annee]
    print(f"  Index {annee}…", end=" ", flush=True)
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    try:
        text = r.content.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = r.content.decode("latin-1")
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    cols = reader.fieldnames
    col_nom = (
        next((c for c in cols if "nom" in c.lower() and "organ" in c.lower()), None)
        or next((c for c in cols if "nom" in c.lower() and "munic" in c.lower()), None)
        or next((c for c in cols if "munic" in c.lower()), cols[1])
    )
    col_url = next((c for c in cols if "url" in c.lower() or "lien" in c.lower()), cols[-1])
    print(f"{len(rows)} muns")
    return {row[col_nom].strip(): row[col_url].strip() for row in rows}


def parse_xml(content, annee, nom_mun, cdname, region):
    root = ET.fromstring(content)
    records = []
    for ue in root.findall("RLUEx"):
        def g(tag, _ue=ue):
            v = _ue.findtext(tag)
            return v.strip() if v else None
        records.append({
            "Annee": annee, "CSDNAME": nom_mun, "CDNAME": cdname, "Region": region,
            "rl0105a": g("RL0105A"), "rl0302a": g("RL0302A"), "rl0307a": g("RL0307A"),
            "rl0308a": g("RL0308A"), "rl0309a": g("RL0309A"), "rl0311a": g("RL0311A"),
            "rl0402a": g("RL0402A"), "rl0403a": g("RL0403A"), "rl0404a": g("RL0404A"),
        })
    return records


def build_role_df(annees, match_df, pause=0.1):
    """Télécharge le rôle de chaque année séparément selon ANNEES/INDEX_URLS."""
    mun_to_mrc  = match_df.set_index("_mun_key")[["CDNAME", "Region"]].to_dict("index")
    muns_voulus = set(mun_to_mrc.keys())
    all_rows, errors = [], []

    for annee in annees:
        print(f"\n══ Année {annee} (rôle {annee}) ══")
        index = fetch_index(annee)
        matched   = [n for n in index if n in muns_voulus]
        unmatched = [n for n in muns_voulus if n not in index]
        print(f"  {len(matched)}/{len(muns_voulus)} muns trouvées")
        if unmatched:
            print(f"  ⚠ Non trouvées: {unmatched[:5]}{'…' if len(unmatched)>5 else ''}")

        for nom_mun, url_xml in index.items():
            if nom_mun not in muns_voulus:
                continue
            meta   = mun_to_mrc[nom_mun]
            cdname = meta["CDNAME"]
            region = meta["Region"]
            print(f"  [{annee}] {nom_mun} ({cdname})…", end=" ", flush=True)
            try:
                r = requests.get(url_xml, timeout=60)
                r.raise_for_status()
                rows = parse_xml(r.content, annee, nom_mun, cdname, region)
                all_rows.extend(rows)
                print(f"{len(rows):,} UE")
            except Exception as e:
                print(f"ERR: {e}")
                errors.append({"annee": annee, "mun": nom_mun, "erreur": str(e)})
            time.sleep(pause)

    if errors:
        print(f"\n⚠ {len(errors)} erreurs:")
        for e in errors: print(f"  {e}")

    df = pd.DataFrame(all_rows)
    num_cols = ["rl0302a","rl0307a","rl0308a","rl0309a","rl0311a","rl0402a","rl0403a","rl0404a"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
    df["rl0105a"] = df["rl0105a"].astype(str)
    print(f"\n✓ Brut: {len(df):,} UE sur {df['Annee'].nunique()} années")
    return df


def classify_types(df):
    d = df.copy()
    d["Types"] = None
    d.loc[(d["rl0311a"]==1) & d["rl0105a"].str.startswith("10"), "Types"] = "Autres immeubles résidentiels"
    d.loc[(d["rl0309a"]==1) & (d["rl0311a"]==1),                "Types"] = "Maisons individuelles détachées"
    d.loc[d["rl0309a"].isin([2,3,4]) & (d["rl0311a"]==1),       "Types"] = "Maisons jumelées ou en rangée"
    d.loc[(d["rl0309a"]==5) & (d["rl0311a"]==1),                "Types"] = "Immeuble comportant deux logements ou plus"
    d.loc[d["rl0311a"]>=2,                                       "Types"] = "Immeuble comportant deux logements ou plus"
    d.loc[d["rl0105a"].str.startswith(tuple(["16","17","18","19"])), "Types"] = "Autres immeubles résidentiels"
    d.loc[d["rl0105a"].str.startswith("11"),                    "Types"] = "Chalet et maison de villégiature"
    d.loc[d["rl0105a"].str.startswith("15"),                    "Types"] = "Habitation en commun"
    d.loc[d["rl0105a"].str.startswith("12"),                    "Types"] = "Maison mobile et roulotte"
    return d


def expand_logements(df):
    d = df.dropna(subset=["rl0311a"]).copy()
    d["rl0311a"] = d["rl0311a"].astype(int)
    return d.loc[d.index.repeat(d["rl0311a"])].reset_index(drop=True)


def categorize_periode(val):
    if pd.isna(val): return "Unknown"
    elif val <= 1960: return "1960 ou avant"
    elif val <= 1980: return "1961-1980"
    elif val <= 2000: return "1981-2000"
    elif val <= 2015: return "2001-2015"
    else: return "2016 et plus"


def save_json(df, path, orient="records"):
    df.to_json(DATA_DIR / path, orient=orient, force_ascii=False, indent=None)
    print(f"  ✓ {path} ({len(df):,} lignes)")


# ── Pipeline principal ─────────────────────────────────────────────────────────

def main():
    MATCH = load_match()

    # 1. Télécharger toutes les années
    Role_brut = build_role_df(ANNEES, MATCH)

    # 2. Filtre résidentiel + classification
    Role_UE = Role_brut[Role_brut["rl0105a"].str.startswith("1")].copy()
    Role_UE = classify_types(Role_UE)

    # 3. Expansion par logement (1 ligne = 1 logement)
    Role_exp = expand_logements(Role_UE)
    print(f"\nAprès expansion: {len(Role_exp):,} logements")

    # ── OUTPUT 1 : Nombre de logements par type — MRC ─────────────────────────
    # (Image 3 & 4 : stacked bar + table)
    mrc_types = (Role_exp
        .groupby(["Annee","CDNAME","Types"])
        .agg(N=("Annee","count"))
        .reset_index()
        .pivot_table(index=["Annee","CDNAME"], columns="Types", values="N", aggfunc="sum")
        .reset_index())
    mrc_types.columns.name = None
    for col in TYPE_COLS:
        if col not in mrc_types.columns: mrc_types[col] = 0
    mrc_types["Total"] = mrc_types[TYPE_COLS].sum(axis=1)
    for col in TYPE_COLS:
        mrc_types[f"{col}_pct"] = (mrc_types[col] / mrc_types["Total"] * 100).round(2)
    save_json(mrc_types, "logements_types_mrc.json")

    # ── OUTPUT 2 : Nombre de logements par type — MUN ─────────────────────────
    # (Image 5 : table mun)
    mun_types = (Role_exp
        .groupby(["Annee","CDNAME","CSDNAME","Types"])
        .agg(N=("Annee","count"))
        .reset_index()
        .rename(columns={"Types":"Types de construction résidentielle","N":"Nombre de logements"}))
    save_json(mun_types, "logements_types_mun.json")

    # ── OUTPUT 3 : Valeur foncière — MRC ──────────────────────────────────────
    # (Image 6 & 8 : bullet bar + table multi-années)
    mrc_val = Role_UE.groupby(["Annee","CDNAME","Types"]).agg(
        terrain=("rl0402a","mean"),
        batiment=("rl0403a","mean"),
        immeuble=("rl0404a","mean"),
    ).reset_index()
    mrc_val_tot = Role_UE.groupby(["Annee","CDNAME"]).agg(
        terrain=("rl0402a","mean"),
        batiment=("rl0403a","mean"),
        immeuble=("rl0404a","mean"),
    ).reset_index()
    mrc_val_tot["Types"] = "Total des unités d'évaluation résidentielles"
    mrc_val = pd.concat([mrc_val, mrc_val_tot], ignore_index=True)
    mrc_val = mrc_val.round(0)
    save_json(mrc_val, "valeur_mrc.json")

    # ── OUTPUT 4 : Valeur foncière — MUN ──────────────────────────────────────
    # (Image 7 : table mun)
    mun_val = Role_UE.groupby(["Annee","CDNAME","CSDNAME","Types"]).agg(
        terrain=("rl0402a","mean"),
        batiment=("rl0403a","mean"),
        immeuble=("rl0404a","mean"),
    ).reset_index().round(0)
    save_json(mun_val, "valeur_mun.json")

    # ── OUTPUT 5 : Âge moyen — MRC ────────────────────────────────────────────
    # (Image 1 : bar chart âge par type)
    mrc_age = Role_UE.groupby(["Annee","CDNAME","Types"]).agg(
        annee_moy=("rl0307a","mean")
    ).reset_index()
    mrc_age_tot = Role_UE.groupby(["Annee","CDNAME"]).agg(
        annee_moy=("rl0307a","mean")
    ).reset_index()
    mrc_age_tot["Types"] = "Total des unités d'évaluation résidentielles"
    mrc_age = pd.concat([mrc_age, mrc_age_tot], ignore_index=True)
    mrc_age["age_moyen"] = (mrc_age["Annee"] - mrc_age["annee_moy"]).round(1)
    save_json(mrc_age, "age_mrc.json")

    # ── OUTPUT 6 : Âge moyen — MUN ────────────────────────────────────────────
    # (Image 2 : table mun)
    mun_age = Role_UE.groupby(["Annee","CDNAME","CSDNAME","Types"]).agg(
        annee_moy=("rl0307a","mean")
    ).reset_index()
    mun_age_tot = Role_UE.groupby(["Annee","CDNAME","CSDNAME"]).agg(
        annee_moy=("rl0307a","mean")
    ).reset_index()
    mun_age_tot["Types"] = "Total des unités d'évaluation résidentielles"
    mun_age = pd.concat([mun_age, mun_age_tot], ignore_index=True)
    mun_age["age_moyen"] = (mun_age["Annee"] - mun_age["annee_moy"]).round(1)
    save_json(mun_age, "age_mun.json")

    # ── OUTPUT 7 : Période de construction — MRC ──────────────────────────────
    Role_UE["Période"] = Role_UE["rl0307a"].apply(categorize_periode)
    mrc_per = Role_UE.groupby(["Annee","CDNAME","Types","Période"]).agg(N=("Annee","count")).reset_index()
    mrc_per = mrc_per[mrc_per["Période"] != "Unknown"]
    save_json(mrc_per, "periode_mrc.json")

    # ── OUTPUT 8 : Liste des MRC et municipalités (pour le filtre UI) ─────────
    mrc_list = MATCH.groupby("CDNAME")["Municipalité"].apply(list).reset_index()
    mrc_list.columns = ["CDNAME","municipalites"]
    # Ajoute région
    region_map = MATCH.drop_duplicates("CDNAME").set_index("CDNAME")["Region"].to_dict()
    mrc_list["Region"] = mrc_list["CDNAME"].map(region_map)
    save_json(mrc_list, "mrc_list.json")

    print("\n🎉 Pipeline terminé. Données dans /data/")


if __name__ == "__main__":
    main()
