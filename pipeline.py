"""
Rôle d'évaluation foncière – Pipeline XML API
Données fiables à partir de 2023 seulement.
Les années futures (2027, 2028, 2029, 2030…) sont captées automatiquement
dès que leurs index sont publiés par le MAMH.
"""
import requests, xml.etree.ElementTree as ET, pandas as pd
import io, time, csv, json, os
from datetime import date
from pathlib import Path

# ── Paramètres ────────────────────────────────────────────────────────────────
ANNEE_MIN = 2019
ANNEE_MAX = date.today().year + 4   # capture automatique des futurs rôles
ANNEES    = list(range(ANNEE_MIN, ANNEE_MAX + 1))
INDEX_URLS = {a: f"https://donneesouvertes.affmunqc.net/role/indexRole{a}.csv" for a in ANNEES}

MATCH_PATH = "MATCH.csv"
DATA_DIR   = Path("web/data")
DATA_DIR.mkdir(exist_ok=True)

TYPE_COLS = [
    "Autres immeubles résidentiels","Chalet et maison de villégiature",
    "Habitation en commun","Immeuble comportant deux logements ou plus",
    "Maison mobile et roulotte","Maisons individuelles détachées","Maisons jumelées ou en rangée",
]

def load_match():
    df = pd.read_csv(MATCH_PATH)
    if len(df.columns)==2: df.columns=["Municipalité","CDNAME"]; df["Region"]=None
    else: df.columns=["Municipalité","CDNAME","Region"]
    df["_mun_key"]=df["Municipalité"].str.strip()
    print(f"MATCH: {len(df)} municipalités, {df['CDNAME'].nunique()} MRC")
    return df

def fetch_index(annee):
    url=INDEX_URLS[annee]; headers={"User-Agent":"Mozilla/5.0"}
    print(f"  Index {annee}…", end=" ", flush=True)
    try: r=requests.get(url,headers=headers,timeout=60)
    except Exception as e: print(f"erreur réseau: {e}"); return None
    if r.status_code in (404,403): print(f"non disponible ({r.status_code}), ignoré"); return None
    r.raise_for_status()
    try: text=r.content.decode("utf-8-sig")
    except: text=r.content.decode("latin-1")
    reader=csv.DictReader(io.StringIO(text)); rows=list(reader); cols=reader.fieldnames or []
    if not rows: print("vide"); return None
    col_nom=(next((c for c in cols if "nom" in c.lower() and "organ" in c.lower()),None)
        or next((c for c in cols if "nom" in c.lower() and "munic" in c.lower()),None)
        or next((c for c in cols if "munic" in c.lower()),cols[1] if len(cols)>1 else cols[0]))
    col_url=next((c for c in cols if "url" in c.lower() or "lien" in c.lower()),cols[-1])
    print(f"{len(rows)} muns")
    return {row[col_nom].strip():row[col_url].strip() for row in rows if row.get(col_nom) and row.get(col_url)}

def parse_xml(content,annee,nom_mun,cdname,region):
    root=ET.fromstring(content); records=[]
    for ue in root.findall("RLUEx"):
        def g(tag,_ue=ue): v=_ue.findtext(tag); return v.strip() if v else None
        records.append({"Annee":annee,"CSDNAME":nom_mun,"CDNAME":cdname,"Region":region,
            "rl0105a":g("RL0105A"),"rl0302a":g("RL0302A"),"rl0307a":g("RL0307A"),
            "rl0308a":g("RL0308A"),"rl0309a":g("RL0309A"),"rl0311a":g("RL0311A"),
            "rl0402a":g("RL0402A"),"rl0403a":g("RL0403A"),"rl0404a":g("RL0404A")})
    return records

def build_role_df(match_df,pause=0.1):
    mun_to_mrc=match_df.set_index("_mun_key")[["CDNAME","Region"]].to_dict("index")
    muns_voulus=set(mun_to_mrc.keys()); all_rows,errors=[],[]
    for annee in ANNEES:
        print(f"\n══ Année {annee} ══")
        index=fetch_index(annee)
        if not index: continue
        matched=[n for n in index if n in muns_voulus]
        print(f"  {len(matched)}/{len(muns_voulus)} muns trouvées")
        for nom_mun,url_xml in index.items():
            if nom_mun not in muns_voulus: continue
            meta=mun_to_mrc[nom_mun]; cdname=meta["CDNAME"]; region=meta["Region"]
            print(f"  [{annee}] {nom_mun}…", end=" ", flush=True)
            try:
                r=requests.get(url_xml,timeout=60); r.raise_for_status()
                rows=parse_xml(r.content,annee,nom_mun,cdname,region)
                all_rows.extend(rows); print(f"{len(rows):,} UE")
            except Exception as e:
                print(f"ERR: {e}"); errors.append({"annee":annee,"mun":nom_mun,"erreur":str(e)})
            time.sleep(pause)
    df=pd.DataFrame(all_rows)
    if df.empty: return df
    num_cols=["rl0302a","rl0307a","rl0308a","rl0309a","rl0311a","rl0402a","rl0403a","rl0404a"]
    df[num_cols]=df[num_cols].apply(pd.to_numeric,errors="coerce")
    df["rl0105a"]=df["rl0105a"].astype(str)
    print(f"\n✓ Brut: {len(df):,} UE sur {df['Annee'].nunique()} années")
    return df

def classify_types(df):
    d=df.copy(); d["Types"]=None
    d.loc[(d["rl0311a"]==1)&d["rl0105a"].str.startswith("10"),"Types"]="Autres immeubles résidentiels"
    d.loc[(d["rl0309a"]==1)&(d["rl0311a"]==1),"Types"]="Maisons individuelles détachées"
    d.loc[d["rl0309a"].isin([2,3,4])&(d["rl0311a"]==1),"Types"]="Maisons jumelées ou en rangée"
    d.loc[(d["rl0309a"]==5)&(d["rl0311a"]==1),"Types"]="Immeuble comportant deux logements ou plus"
    d.loc[d["rl0311a"]>=2,"Types"]="Immeuble comportant deux logements ou plus"
    d.loc[d["rl0105a"].str.startswith(tuple(["16","17","18","19"])),"Types"]="Autres immeubles résidentiels"
    d.loc[d["rl0105a"].str.startswith("11"),"Types"]="Chalet et maison de villégiature"
    d.loc[d["rl0105a"].str.startswith("15"),"Types"]="Habitation en commun"
    d.loc[d["rl0105a"].str.startswith("12"),"Types"]="Maison mobile et roulotte"
    return d

def expand_logements(df):
    d=df.dropna(subset=["rl0311a"]).copy(); d["rl0311a"]=d["rl0311a"].astype(int)
    return d.loc[d.index.repeat(d["rl0311a"])].reset_index(drop=True)

def categorize_periode(val):
    if pd.isna(val): return None
    elif val<=1960: return "1960 ou avant"
    elif val<=1980: return "1961-1980"
    elif val<=2000: return "1981-2000"
    elif val<=2015: return "2001-2015"
    else: return "2016 et plus"

def save_json(df,path):
    df.to_json(DATA_DIR/path,orient="records",force_ascii=False,indent=None)
    print(f"  ✓ {path} ({len(df):,} lignes)")

def main():
    MATCH=load_match()
    Role_brut=build_role_df(MATCH)
    if Role_brut.empty: print("Aucune donnée."); return

    Role_UE=Role_brut[Role_brut["rl0105a"].str.startswith("1")].copy()
    Role_UE=classify_types(Role_UE)
    Role_exp=expand_logements(Role_UE)
    print(f"\nAprès expansion: {len(Role_exp):,} logements")

    # 1. Logements MRC
    mrc_types=(Role_exp.groupby(["Annee","CDNAME","Types"]).agg(N=("Annee","count")).reset_index()
        .pivot_table(index=["Annee","CDNAME"],columns="Types",values="N",aggfunc="sum").reset_index())
    mrc_types.columns.name=None
    for col in TYPE_COLS:
        if col not in mrc_types.columns: mrc_types[col]=0
    mrc_types["Total"]=mrc_types[TYPE_COLS].sum(axis=1)
    for col in TYPE_COLS: mrc_types[f"{col}_pct"]=(mrc_types[col]/mrc_types["Total"]*100).round(2)
    save_json(mrc_types,"logements_types_mrc.json")

    # 2. Logements MUN
    save_json(Role_exp.groupby(["Annee","CDNAME","CSDNAME","Types"]).agg(N=("Annee","count")).reset_index()
        .rename(columns={"Types":"Types de construction résidentielle","N":"Nombre de logements"}),
        "logements_types_mun.json")

    # 3. Valeur MRC
    mrc_val=Role_UE.groupby(["Annee","CDNAME","Types"]).agg(terrain=("rl0402a","mean"),batiment=("rl0403a","mean"),immeuble=("rl0404a","mean")).reset_index()
    tot=Role_UE.groupby(["Annee","CDNAME"]).agg(terrain=("rl0402a","mean"),batiment=("rl0403a","mean"),immeuble=("rl0404a","mean")).reset_index()
    tot["Types"]="Total des unités d'évaluation résidentielles"
    save_json(pd.concat([mrc_val,tot],ignore_index=True).round(0),"valeur_mrc.json")

    # 4. Valeur MUN
    save_json(Role_UE.groupby(["Annee","CDNAME","CSDNAME","Types"]).agg(terrain=("rl0402a","mean"),batiment=("rl0403a","mean"),immeuble=("rl0404a","mean")).reset_index().round(0),"valeur_mun.json")

    # 5. Âge MRC
    mrc_age=Role_UE.groupby(["Annee","CDNAME","Types"]).agg(annee_moy=("rl0307a","mean")).reset_index()
    tot_age=Role_UE.groupby(["Annee","CDNAME"]).agg(annee_moy=("rl0307a","mean")).reset_index()
    tot_age["Types"]="Total des unités d'évaluation résidentielles"
    mrc_age=pd.concat([mrc_age,tot_age],ignore_index=True)
    mrc_age["age_moyen"]=(mrc_age["Annee"]-mrc_age["annee_moy"]).round(1)
    save_json(mrc_age,"age_mrc.json")

    # 6. Âge MUN
    mun_age=Role_UE.groupby(["Annee","CDNAME","CSDNAME","Types"]).agg(annee_moy=("rl0307a","mean")).reset_index()
    tot_age_mun=Role_UE.groupby(["Annee","CDNAME","CSDNAME"]).agg(annee_moy=("rl0307a","mean")).reset_index()
    tot_age_mun["Types"]="Total des unités d'évaluation résidentielles"
    mun_age=pd.concat([mun_age,tot_age_mun],ignore_index=True)
    mun_age["age_moyen"]=(mun_age["Annee"]-mun_age["annee_moy"]).round(1)
    save_json(mun_age,"age_mun.json")

    # 7. Période MRC
    Role_UE["Période"]=Role_UE["rl0307a"].apply(categorize_periode)
    save_json(Role_UE.dropna(subset=["Période"]).groupby(["Annee","CDNAME","Types","Période"]).agg(N=("Annee","count")).reset_index(),"periode_mrc.json")

    # 8. Période MUN (NEW)
    save_json(Role_UE.dropna(subset=["Période"]).groupby(["Annee","CDNAME","CSDNAME","Types","Période"]).agg(N=("Annee","count")).reset_index(),"periode_mun.json")

    # 9. Superficie MRC — méthodologie moyenne demandée
    mrc_sup = Role_UE.groupby(["Annee", "CDNAME", "Types"]).agg(
        superficie_terrain=("rl0302a", "mean"),
        aire_etages=("rl0308a", "mean")
    ).reset_index()
    tot_sup = Role_UE.groupby(["Annee", "CDNAME"]).agg(
        superficie_terrain=("rl0302a", "mean"),
        aire_etages=("rl0308a", "mean")
    ).reset_index()
    tot_sup["Types"] = "Total des unités d'évaluation résidentielles"
    save_json(pd.concat([mrc_sup, tot_sup], ignore_index=True).round(1), "superficie_mrc.json")

    # 10. Superficie MUN — mêmes moyennes par municipalité
    mun_sup = Role_UE.groupby(["Annee", "CDNAME", "CSDNAME", "Types"]).agg(
        superficie_terrain=("rl0302a", "mean"),
        aire_etages=("rl0308a", "mean")
    ).reset_index()
    tot_sup_mun = Role_UE.groupby(["Annee", "CDNAME", "CSDNAME"]).agg(
        superficie_terrain=("rl0302a", "mean"),
        aire_etages=("rl0308a", "mean")
    ).reset_index()
    tot_sup_mun["Types"] = "Total des unités d'évaluation résidentielles"
    save_json(pd.concat([mun_sup, tot_sup_mun], ignore_index=True).round(1), "superficie_mun.json")

    # 11. MRC list
    mrc_list=MATCH.groupby("CDNAME")["Municipalité"].apply(list).reset_index()
    mrc_list.columns=["CDNAME","municipalites"]
    region_map=MATCH.drop_duplicates("CDNAME").set_index("CDNAME")["Region"].to_dict()
    mrc_list["Region"]=mrc_list["CDNAME"].map(region_map)
    save_json(mrc_list,"mrc_list.json")
    print("\n🎉 Pipeline terminé.")

if __name__=="__main__":
    main()
