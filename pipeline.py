"""
Rôle d'évaluation foncière – Pipeline de données
================================================
Sources selon l'année :
  • 2023 et plus  : API MAMH (index CSV + XML par municipalité)
  • 2012 à 2022   : SHP provincial (ROLE{YYYY}_SHP.zip)
    → téléchargé automatiquement par le workflow GitHub Actions
    → un seul passage de lecture sur ~3,6 M d'enregistrements (~8 s par année)
    → la correspondance code_mun → CSDNAME/CDNAME est fournie par
      le fichier pf-mun-2023-2023.csv (à placer dans le répertoire racine)

Municipalités couvertes : celles dont le CDNAME figure dans MATCH.csv.
Les municipalités absentes du SHP (territoires non organisés, Premières Nations,
noms historiques différents) sont ignorées silencieusement.
"""
import requests, xml.etree.ElementTree as ET, pandas as pd
import io, time, csv, json, os, zipfile, re, struct
from datetime import date
from pathlib import Path

# ── Paramètres ────────────────────────────────────────────────────────────────
ANNEE_MIN = 2012
ANNEE_MAX = date.today().year + 4
ANNEES    = list(range(ANNEE_MIN, ANNEE_MAX + 1))

# API MAMH pour 2023+
INDEX_URLS = {a: f"https://donneesouvertes.affmunqc.net/role/indexRole{a}.csv"
              for a in ANNEES if a >= 2023}

# SHP zips détectés automatiquement dans le répertoire courant
# Convention de nommage : ROLE{YYYY}_SHP.zip
SHP_ZIPS = {
    int(m.group(1)): Path(p)
    for p in Path(".").glob("ROLE*_SHP.zip")
    for m in [re.search(r"(\d{4})", Path(p).name)]
    if m
}

MATCH_PATH  = "MATCH.csv"
PF_MUN_PATH = "pf-mun-2023-2023.csv"   # référentiel code_mun → nom_mun / nom_mrc
DATA_DIR    = Path("web/data")
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

# ── Chargement des référentiels ────────────────────────────────────────────────
def load_match():
    df = pd.read_csv(MATCH_PATH)
    if len(df.columns) == 2:
        df.columns = ["Municipalité", "CDNAME"]; df["Region"] = None
    else:
        df.columns = ["Municipalité", "CDNAME", "Region"]
    df["_mun_key"] = df["Municipalité"].str.strip()
    print(f"MATCH: {len(df)} municipalités, {df['CDNAME'].nunique()} MRC")
    return df


def load_pf_mun(our_mrcs: set) -> dict:
    """
    Charge pf-mun-2023-2023.csv et retourne un dict :
        code_mun (str 5 chiffres) → (CSDNAME, CDNAME)
    Filtre sur nos MRC cibles et applique les corrections de noms connues.
    """
    p = Path(PF_MUN_PATH)
    if not p.exists():
        print(f"  ⚠  {PF_MUN_PATH} introuvable – traitement des SHP désactivé.")
        return {}

    lookup = {}
    with open(p, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            code = row["cod_geo_n"].strip()
            if not (code.isdigit() and len(code) == 5):
                continue

            nom_mun = row["nom_mun"].strip()
            nom_mrc = row["nom_mrc"].strip()

            # Gatineau (ville) est classée « Hors MRC » dans pf-mun
            if "Hors MRC - Gatineau" in nom_mrc:
                nom_mrc = "Gatineau"

            # Clarenceville a été renommée Saint-Georges-de-Clarenceville
            if nom_mun == "Clarenceville" and "Haut-Richelieu" in nom_mrc:
                nom_mun = "Saint-Georges-de-Clarenceville"

            if nom_mrc in our_mrcs:
                lookup[code] = (nom_mun, nom_mrc)

    print(f"pf-mun : {len(lookup)} codes pour nos {len(our_mrcs)} MRC")
    return lookup


# ── Parsing XML (API MAMH) ─────────────────────────────────────────────────────
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


# ── Source 1 : API MAMH (2023+) ────────────────────────────────────────────────
def fetch_index(annee):
    url = INDEX_URLS[annee]
    headers = {"User-Agent": "Mozilla/5.0"}
    print(f"  Index {annee}…", end=" ", flush=True)
    try:
        r = requests.get(url, headers=headers, timeout=60)
    except Exception as e:
        print(f"erreur réseau: {e}"); return None
    if r.status_code in (404, 403):
        print(f"non disponible ({r.status_code}), ignoré"); return None
    r.raise_for_status()
    try:
        text = r.content.decode("utf-8-sig")
    except:
        text = r.content.decode("latin-1")
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    cols = reader.fieldnames or []
    if not rows:
        print("vide"); return None
    col_nom = (
        next((c for c in cols if "nom" in c.lower() and "organ" in c.lower()), None)
        or next((c for c in cols if "nom" in c.lower() and "munic" in c.lower()), None)
        or next((c for c in cols if "munic" in c.lower()), cols[1] if len(cols) > 1 else cols[0])
    )
    col_url = next((c for c in cols if "url" in c.lower() or "lien" in c.lower()), cols[-1])
    print(f"{len(rows)} muns")
    return {row[col_nom].strip(): row[col_url].strip()
            for row in rows if row.get(col_nom) and row.get(col_url)}


def build_role_df_api(match_df, pause=0.1):
    """Télécharge les données via l'API MAMH (2023 et plus)."""
    mun_to_mrc = match_df.set_index("_mun_key")[["CDNAME", "Region"]].to_dict("index")
    muns_voulus = set(mun_to_mrc.keys())
    all_rows, errors = [], []

    annees_api = [a for a in ANNEES if a >= 2023 and a not in SHP_ZIPS]
    for annee in annees_api:
        print(f"\n══ Année {annee} (API) ══")
        index = fetch_index(annee)
        if not index:
            continue
        matched = [n for n in index if n in muns_voulus]
        print(f"  {len(matched)}/{len(muns_voulus)} muns trouvées")
        for nom_mun, url_xml in index.items():
            if nom_mun not in muns_voulus:
                continue
            meta = mun_to_mrc[nom_mun]
            print(f"  [{annee}] {nom_mun}…", end=" ", flush=True)
            try:
                r = requests.get(url_xml, timeout=60)
                r.raise_for_status()
                rows = parse_xml(r.content, annee, nom_mun, meta["CDNAME"], meta["Region"])
                all_rows.extend(rows)
                print(f"{len(rows):,} UE")
            except Exception as e:
                print(f"ERR: {e}")
                errors.append({"annee": annee, "mun": nom_mun, "erreur": str(e)})
            time.sleep(pause)

    return all_rows, errors


# ── Source 2 : SHP provincial (2012–2022) ─────────────────────────────────────
def _read_dbf_layout(f):
    """Lit l'en-tête DBF et retourne (num_records, header_size, record_size, offsets)."""
    header = f.read(32)
    num_records  = struct.unpack("<I", header[4:8])[0]
    header_size  = struct.unpack("<H", header[8:10])[0]
    record_size  = struct.unpack("<H", header[10:12])[0]
    fields = []
    f.seek(32)
    while True:
        fd = f.read(32)
        if not fd or fd[0] == 0x0D:
            break
        name   = fd[:11].replace(b"\x00", b"").decode("latin-1").strip()
        length = fd[16]
        fields.append((name, length))
    offsets = {}
    pos = 1
    for name, length in fields:
        offsets[name] = (pos, length)
        pos += length
    return num_records, header_size, record_size, offsets


def build_role_df_shp(pf_lookup: dict, match_df):
    """
    Lit B05EX1_B05V_UNITE_EVALN.dbf depuis chaque SHP zip détecté,
    filtre par nos MRC via pf_lookup, et retourne les enregistrements bruts.
    """
    if not pf_lookup:
        return [], []

    region_map = match_df.drop_duplicates("CDNAME").set_index("CDNAME")["Region"].to_dict()
    WANTED = ["code_mun", "rl0105a", "rl0302a", "rl0307a", "rl0308a",
              "rl0309a", "rl0311a", "rl0402a", "rl0403a", "rl0404a"]

    all_rows, errors = [], []

    for annee, zip_path in sorted(SHP_ZIPS.items()):
        print(f"\n══ Année {annee} (SHP : {zip_path.name}) ══")
        if not zip_path.exists():
            print(f"  ⚠  Zip introuvable : {zip_path}"); continue

        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                # Trois conventions de nommage selon l'année :
                # 2017-2022 : B05EX1_B05V_UNITE_EVALN.dbf
                # 2019      : UNITE_EVALN.dbf
                # 2012-2016 : B05V_UNITE_EVALN.dbf
                DBF_SUFFIXES = (
                    "B05EX1_B05V_UNITE_EVALN.DBF",
                    "B05V_UNITE_EVALN.DBF",
                    "UNITE_EVALN.DBF",
                )
                dbf_entry = next(
                    (n for n in zf.namelist()
                     if any(n.upper().endswith(s) for s in DBF_SUFFIXES)),
                    None,
                )
                if not dbf_entry:
                    print(f"  ⚠  DBF principal introuvable dans {zip_path.name}"); continue

                print(f"  Décompression de {dbf_entry}…", flush=True)
                with zf.open(dbf_entry) as raw:
                    import io as _io
                    data = _io.BytesIO(raw.read())

            num_records, header_size, record_size, offsets = _read_dbf_layout(data)
            print(f"  {num_records:,} enregistrements")

            code_pos, code_len = offsets["code_mun"]
            rows_year = 0
            found_codes: set = set()

            data.seek(header_size)
            for _ in range(num_records):
                rb = data.read(record_size)
                if not rb or rb[0] == 0x1A:
                    break
                if rb[0] == 0x2A:       # enregistrement supprimé
                    continue

                code = rb[code_pos:code_pos + code_len].decode("latin-1").strip()
                if code not in pf_lookup:
                    continue

                nom_mun, cdname = pf_lookup[code]
                found_codes.add(code)

                def get(field):
                    if field not in offsets: return None
                    pos, ln = offsets[field]
                    v = rb[pos:pos + ln].decode("latin-1").strip()
                    return v if v else None

                all_rows.append({
                    "Annee":   annee,
                    "CSDNAME": nom_mun,
                    "CDNAME":  cdname,
                    "Region":  region_map.get(cdname),
                    "rl0105a": get("rl0105a"),
                    "rl0302a": get("rl0302a"),
                    "rl0307a": get("rl0307a"),
                    "rl0308a": get("rl0308a"),
                    "rl0309a": get("rl0309a"),
                    "rl0311a": get("rl0311a"),
                    "rl0402a": get("rl0402a"),
                    "rl0403a": get("rl0403a"),
                    "rl0404a": get("rl0404a"),
                })
                rows_year += 1

            print(f"  ✓ {rows_year:,} UE retenues ({len(found_codes)} municipalités)")

        except Exception as e:
            print(f"  ERR SHP {annee}: {e}")
            errors.append({"annee": annee, "mun": "SHP", "erreur": str(e)})

    return all_rows, errors


# ── Transformations ────────────────────────────────────────────────────────────
def classify_types(df):
    d = df.copy()
    d["Types"] = None
    d.loc[(d["rl0311a"] == 1) & d["rl0105a"].str.startswith("10"), "Types"] = "Autres immeubles résidentiels"
    d.loc[(d["rl0309a"] == 1) & (d["rl0311a"] == 1), "Types"] = "Maisons individuelles détachées"
    d.loc[d["rl0309a"].isin([2, 3, 4]) & (d["rl0311a"] == 1), "Types"] = "Maisons jumelées ou en rangée"
    d.loc[(d["rl0309a"] == 5) & (d["rl0311a"] == 1), "Types"] = "Immeuble comportant deux logements ou plus"
    d.loc[d["rl0311a"] >= 2, "Types"] = "Immeuble comportant deux logements ou plus"
    d.loc[d["rl0105a"].str.startswith(tuple(["16", "17", "18", "19"])), "Types"] = "Autres immeubles résidentiels"
    d.loc[d["rl0105a"].str.startswith("11"), "Types"] = "Chalet et maison de villégiature"
    d.loc[d["rl0105a"].str.startswith("15"), "Types"] = "Habitation en commun"
    d.loc[d["rl0105a"].str.startswith("12"), "Types"] = "Maison mobile et roulotte"
    return d


def expand_logements(df):
    d = df.dropna(subset=["rl0311a"]).copy()
    d["rl0311a"] = d["rl0311a"].astype(int)
    return d.loc[d.index.repeat(d["rl0311a"])].reset_index(drop=True)


def categorize_periode(val):
    if pd.isna(val):    return None
    elif val <= 1960:   return "1960 ou avant"
    elif val <= 1980:   return "1961-1980"
    elif val <= 2000:   return "1981-2000"
    elif val <= 2015:   return "2001-2015"
    else:               return "2016 et plus"


def save_json(df, path):
    df.to_json(DATA_DIR / path, orient="records", force_ascii=False, indent=None)
    print(f"  ✓ {path} ({len(df):,} lignes)")


# ── Point d'entrée ─────────────────────────────────────────────────────────────
def main():
    MATCH    = load_match()
    our_mrcs = set(MATCH["CDNAME"].unique())
    pf_lookup = load_pf_mun(our_mrcs)

    print(f"\nSHP zips détectés : {dict(sorted(SHP_ZIPS.items())) or 'aucun'}")

    rows_api, _ = build_role_df_api(MATCH)
    rows_shp, _ = build_role_df_shp(pf_lookup, MATCH)
    all_rows = rows_api + rows_shp

    if not all_rows:
        print("Aucune donnée collectée."); return

    Role_brut = pd.DataFrame(all_rows)
    num_cols = ["rl0302a", "rl0307a", "rl0308a", "rl0309a", "rl0311a",
                "rl0402a", "rl0403a", "rl0404a"]
    Role_brut[num_cols] = Role_brut[num_cols].apply(pd.to_numeric, errors="coerce")
    Role_brut["rl0105a"] = Role_brut["rl0105a"].fillna("").astype(str)

    annees = sorted(Role_brut["Annee"].unique())
    print(f"\n✓ Brut : {len(Role_brut):,} UE sur {len(annees)} années ({annees})")

    Role_UE  = Role_brut[Role_brut["rl0105a"].str.startswith("1")].copy()
    Role_UE  = classify_types(Role_UE)
    Role_exp = expand_logements(Role_UE)
    print(f"Après expansion logements : {len(Role_exp):,}")

    # 1. Logements MRC
    mrc_types = (
        Role_exp.groupby(["Annee", "CDNAME", "Types"])
        .agg(N=("Annee", "count")).reset_index()
        .pivot_table(index=["Annee", "CDNAME"], columns="Types", values="N", aggfunc="sum")
        .reset_index()
    )
    mrc_types.columns.name = None
    for col in TYPE_COLS:
        if col not in mrc_types.columns: mrc_types[col] = 0
    mrc_types["Total"] = mrc_types[TYPE_COLS].sum(axis=1)
    for col in TYPE_COLS:
        mrc_types[f"{col}_pct"] = (mrc_types[col] / mrc_types["Total"] * 100).round(2)
    save_json(mrc_types, "logements_types_mrc.json")

    # 2. Logements MUN
    save_json(
        Role_exp.groupby(["Annee", "CDNAME", "CSDNAME", "Types"])
        .agg(N=("Annee", "count")).reset_index()
        .rename(columns={"Types": "Types de construction résidentielle",
                         "N": "Nombre de logements"}),
        "logements_types_mun.json",
    )

    # 3. Valeur MRC
    mrc_val = Role_UE.groupby(["Annee", "CDNAME", "Types"]).agg(
        terrain=("rl0402a", "mean"), batiment=("rl0403a", "mean"), immeuble=("rl0404a", "mean")
    ).reset_index()
    tot = Role_UE.groupby(["Annee", "CDNAME"]).agg(
        terrain=("rl0402a", "mean"), batiment=("rl0403a", "mean"), immeuble=("rl0404a", "mean")
    ).reset_index()
    tot["Types"] = "Total des unités d'évaluation résidentielles"
    save_json(pd.concat([mrc_val, tot], ignore_index=True).round(0), "valeur_mrc.json")

    # 4. Valeur MUN
    save_json(
        Role_UE.groupby(["Annee", "CDNAME", "CSDNAME", "Types"]).agg(
            terrain=("rl0402a", "mean"), batiment=("rl0403a", "mean"), immeuble=("rl0404a", "mean")
        ).reset_index().round(0),
        "valeur_mun.json",
    )

    # 5. Âge MRC
    mrc_age = Role_UE.groupby(["Annee", "CDNAME", "Types"]).agg(annee_moy=("rl0307a", "mean")).reset_index()
    tot_age = Role_UE.groupby(["Annee", "CDNAME"]).agg(annee_moy=("rl0307a", "mean")).reset_index()
    tot_age["Types"] = "Total des unités d'évaluation résidentielles"
    mrc_age = pd.concat([mrc_age, tot_age], ignore_index=True)
    mrc_age["age_moyen"] = (mrc_age["Annee"] - mrc_age["annee_moy"]).round(1)
    save_json(mrc_age, "age_mrc.json")

    # 6. Âge MUN
    mun_age = Role_UE.groupby(["Annee", "CDNAME", "CSDNAME", "Types"]).agg(annee_moy=("rl0307a", "mean")).reset_index()
    tot_age_mun = Role_UE.groupby(["Annee", "CDNAME", "CSDNAME"]).agg(annee_moy=("rl0307a", "mean")).reset_index()
    tot_age_mun["Types"] = "Total des unités d'évaluation résidentielles"
    mun_age = pd.concat([mun_age, tot_age_mun], ignore_index=True)
    mun_age["age_moyen"] = (mun_age["Annee"] - mun_age["annee_moy"]).round(1)
    save_json(mun_age, "age_mun.json")

    # 7. Période MRC
    Role_UE["Période"] = Role_UE["rl0307a"].apply(categorize_periode)
    save_json(
        Role_UE.dropna(subset=["Période"])
        .groupby(["Annee", "CDNAME", "Types", "Période"]).agg(N=("Annee", "count")).reset_index(),
        "periode_mrc.json",
    )

    # 8. Période MUN
    save_json(
        Role_UE.dropna(subset=["Période"])
        .groupby(["Annee", "CDNAME", "CSDNAME", "Types", "Période"]).agg(N=("Annee", "count")).reset_index(),
        "periode_mun.json",
    )

    # 9. Superficie MRC
    mrc_sup = Role_UE.groupby(["Annee", "CDNAME", "Types"]).agg(
        superficie_terrain=("rl0302a", "mean"), aire_etages=("rl0308a", "mean")).reset_index()
    tot_sup = Role_UE.groupby(["Annee", "CDNAME"]).agg(
        superficie_terrain=("rl0302a", "mean"), aire_etages=("rl0308a", "mean")).reset_index()
    tot_sup["Types"] = "Total des unités d'évaluation résidentielles"
    save_json(pd.concat([mrc_sup, tot_sup], ignore_index=True).round(1), "superficie_mrc.json")

    # 10. Superficie MUN
    mun_sup = Role_UE.groupby(["Annee", "CDNAME", "CSDNAME", "Types"]).agg(
        superficie_terrain=("rl0302a", "mean"), aire_etages=("rl0308a", "mean")).reset_index()
    tot_sup_mun = Role_UE.groupby(["Annee", "CDNAME", "CSDNAME"]).agg(
        superficie_terrain=("rl0302a", "mean"), aire_etages=("rl0308a", "mean")).reset_index()
    tot_sup_mun["Types"] = "Total des unités d'évaluation résidentielles"
    save_json(pd.concat([mun_sup, tot_sup_mun], ignore_index=True).round(1), "superficie_mun.json")

    # 11. Liste MRC
    mrc_list = MATCH.groupby("CDNAME")["Municipalité"].apply(list).reset_index()
    mrc_list.columns = ["CDNAME", "municipalites"]
    region_map = MATCH.drop_duplicates("CDNAME").set_index("CDNAME")["Region"].to_dict()
    mrc_list["Region"] = mrc_list["CDNAME"].map(region_map)
    save_json(mrc_list, "mrc_list.json")

    print("\n🎉 Pipeline terminé.")


if __name__ == "__main__":
    main()
