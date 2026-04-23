"""
Rôle d'évaluation foncière – Pipeline de données
================================================
Sources selon l'année :
  • 2023 et plus  : API MAMH (index CSV + XML par municipalité)
  • 2012 à 2022   : SHP provincial (ROLE{YYYY}_SHP.zip)
  • Indicateurs PU: Role_{YYYY}_PU.zip
    Correction plan complémentaire : dissolution par bâtiment (mat18[:15]),
    sum(rl0302a) = empreinte réelle, sum(rl0311a) = total logements.
"""
import requests, xml.etree.ElementTree as ET, pandas as pd
import io, time, csv, json, os, zipfile, re, struct
import numpy as np
from datetime import date
from pathlib import Path
ANNEE_MIN = 2012
ANNEE_MAX = date.today().year + 4
ANNEES    = list(range(ANNEE_MIN, ANNEE_MAX + 1))
INDEX_URLS = {a: f"https://donneesouvertes.affmunqc.net/role/indexRole{a}.csv"
              for a in ANNEES if a >= 2023}
SHP_ZIPS = {}
for _p in Path(".").glob("ROLE*_SHP.zip"):
    _m = re.search(r"(\d{4})", _p.name)
    if _m: SHP_ZIPS[int(_m.group(1))] = _p
PU_ZIPS = {}
for _p in Path(".").glob("Role_*_PU.zip"):
    _m = re.search(r"(\d{4})", _p.name)
    if _m: PU_ZIPS[int(_m.group(1))] = _p
MATCH_PATH  = "MATCH.csv"
PF_MUN_PATH = "pf-mun-2023-2023.csv"
DATA_DIR    = Path("web/data")
DATA_DIR.mkdir(exist_ok=True)
TYPE_COLS = [
    "Maisons individuelles détachées",
    "Maisons jumelées ou en rangée",
    "Logements dans un immeuble comportant deux logements ou plus",
    "Chalet et maison de villégiature",
    "Habitation en commun",
    "Maison mobile et roulotte",
    "Autres immeubles résidentiels",
]
def load_match():
    df = pd.read_csv(MATCH_PATH)
    if len(df.columns)==2: df.columns=["Municipalité","CDNAME"]; df["Region"]=None
    else: df.columns=["Municipalité","CDNAME","Region"]
    df["_mun_key"]=df["Municipalité"].str.strip()
    print(f"MATCH: {len(df)} municipalités, {df['CDNAME'].nunique()} MRC")
    return df
def load_pf_mun(our_mrcs):
    p=Path(PF_MUN_PATH)
    if not p.exists(): print(f"  ⚠  {PF_MUN_PATH} introuvable"); return {}
    lookup={}
    with open(p,encoding="utf-8-sig",newline="") as f:
        for row in csv.DictReader(f):
            code=row["cod_geo_n"].strip()
            if not (code.isdigit() and len(code)==5): continue
            nom_mun=row["nom_mun"].strip(); nom_mrc=row["nom_mrc"].strip()
            if "Hors MRC - Gatineau" in nom_mrc: nom_mrc="Gatineau"
            if nom_mun=="Clarenceville" and "Haut-Richelieu" in nom_mrc:
                nom_mun="Saint-Georges-de-Clarenceville"
            if nom_mrc in our_mrcs: lookup[code]=(nom_mun,nom_mrc)
    print(f"pf-mun : {len(lookup)} codes pour nos {len(our_mrcs)} MRC")
    return lookup
def parse_xml(content,annee,nom_mun,cdname,region):
    root=ET.fromstring(content); records=[]
    for ue in root.findall("RLUEx"):
        def g(tag,_ue=ue):
            v=_ue.findtext(tag); return v.strip() if v else None
        records.append({"Annee":annee,"CSDNAME":nom_mun,"CDNAME":cdname,"Region":region,
            "rl0105a":g("RL0105A"),"rl0302a":g("RL0302A"),"rl0307a":g("RL0307A"),
            "rl0308a":g("RL0308A"),"rl0309a":g("RL0309A"),"rl0311a":g("RL0311A"),
            "rl0402a":g("RL0402A"),"rl0403a":g("RL0403A"),"rl0404a":g("RL0404A")})
    return records
def fetch_index(annee):
    url=INDEX_URLS[annee]; headers={"User-Agent":"Mozilla/5.0"}
    print(f"  Index {annee}…",end=" ",flush=True)
    try: r=requests.get(url,headers=headers,timeout=60)
    except Exception as e: print(f"erreur: {e}"); return None
    if r.status_code in (404,403): print(f"non disponible ({r.status_code})"); return None
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
def build_role_df_api(match_df,pause=0.1):
    mun_to_mrc=match_df.set_index("_mun_key")[["CDNAME","Region"]].to_dict("index")
    muns_voulus=set(mun_to_mrc.keys()); all_rows,errors=[],[]
    for annee in [a for a in ANNEES if a>=2023 and a not in SHP_ZIPS]:
        print(f"\n══ Année {annee} (API) ══")
        index=fetch_index(annee)
        if not index: continue
        print(f"  {len([n for n in index if n in muns_voulus])}/{len(muns_voulus)} muns trouvées")
        for nom_mun,url_xml in index.items():
            if nom_mun not in muns_voulus: continue
            meta=mun_to_mrc[nom_mun]
            print(f"  [{annee}] {nom_mun}…",end=" ",flush=True)
            try:
                r=requests.get(url_xml,timeout=60); r.raise_for_status()
                rows=parse_xml(r.content,annee,nom_mun,meta["CDNAME"],meta["Region"])
                all_rows.extend(rows); print(f"{len(rows):,} UE")
            except Exception as e:
                print(f"ERR: {e}"); errors.append({"annee":annee,"mun":nom_mun,"erreur":str(e)})
            time.sleep(pause)
    return all_rows,errors
def _read_dbf_layout(f):
    header=f.read(32)
    num_records=struct.unpack("<I",header[4:8])[0]
    header_size=struct.unpack("<H",header[8:10])[0]
    record_size=struct.unpack("<H",header[10:12])[0]
    fields=[]; f.seek(32)
    while True:
        fd=f.read(32)
        if not fd or fd[0]==0x0D: break
        name=fd[:11].replace(b"\x00",b"").decode("latin-1").strip().lower()
        fields.append((name,fd[16]))
    offsets={}; pos=1
    for name,length in fields:
        offsets[name]=(pos,length); pos+=length
    return num_records,header_size,record_size,offsets
def build_role_df_shp(pf_lookup,match_df):
    if not pf_lookup: return [],[]
    region_map=match_df.drop_duplicates("CDNAME").set_index("CDNAME")["Region"].to_dict()
    all_rows,errors=[],[]
    for annee,zip_path in sorted(SHP_ZIPS.items()):
        print(f"\n══ Année {annee} (SHP : {zip_path.name}) ══")
        if not zip_path.exists(): print(f"  ⚠  Zip introuvable"); continue
        try:
            with zipfile.ZipFile(zip_path,"r") as zf:
                def is_main_dbf(name):
                    u=name.upper()
                    if "ADR_UNITE_EVALN" in u: return False
                    return (u.endswith("B05EX1_B05V_UNITE_EVALN.DBF") or
                            u.endswith("B05V_UNITE_EVALN.DBF") or
                            u.endswith("UNITE_EVALN.DBF"))
                dbf_entry=next((n for n in zf.namelist() if is_main_dbf(n)),None)
                if not dbf_entry: print(f"  ⚠  DBF introuvable"); continue
                print(f"  Décompression de {dbf_entry}…",flush=True)
                with zf.open(dbf_entry) as raw:
                    import io as _io; data=_io.BytesIO(raw.read())
            num_records,header_size,record_size,offsets=_read_dbf_layout(data)
            print(f"  {num_records:,} enregistrements")
            code_pos,code_len=offsets["code_mun"]
            rows_year=0; found_codes=set()
            data.seek(header_size)
            for _ in range(num_records):
                rb=data.read(record_size)
                if not rb or rb[0]==0x1A: break
                if rb[0]==0x2A: continue
                code=rb[code_pos:code_pos+code_len].decode("latin-1").strip()
                if code not in pf_lookup: continue
                nom_mun,cdname=pf_lookup[code]; found_codes.add(code)
                def get(field):
                    if field not in offsets: return None
                    pos,ln=offsets[field]; v=rb[pos:pos+ln].decode("latin-1").strip()
                    return v if v else None
                all_rows.append({"Annee":annee,"CSDNAME":nom_mun,"CDNAME":cdname,
                    "Region":region_map.get(cdname),"rl0105a":get("rl0105a"),
                    "rl0302a":get("rl0302a"),"rl0307a":get("rl0307a"),"rl0308a":get("rl0308a"),
                    "rl0309a":get("rl0309a"),"rl0311a":get("rl0311a"),"rl0402a":get("rl0402a"),
                    "rl0403a":get("rl0403a"),"rl0404a":get("rl0404a")})
                rows_year+=1
            if rows_year<200000:
                print(f"  ⚠  {rows_year:,} UE – zip suspect, ignoré.")
                all_rows=[r for r in all_rows if r["Annee"]!=annee]
            else:
                print(f"  ✓ {rows_year:,} UE retenues ({len(found_codes)} municipalités)")
        except Exception as e:
            print(f"  ERR SHP {annee}: {e}"); errors.append({"annee":annee,"mun":"SHP","erreur":str(e)})
    return all_rows,errors
def build_indicateurs_pu(pf_lookup):
    """
    Indicateurs stratégiques – Périmètres d'urbanisation.
    Correction plan complémentaire (PDF Annexe 1) :
      - Chaque unité de condo (rl0310a=5) a sa quote-part de terrain dans rl0302a.
      - Ces fractions s'additionnent à l'empreinte réelle du bâtiment.
      - Regroupement par bâtiment : mat18[:15] = identifiant unique du plan complémentaire.
      - sum(rl0302a) par groupe = empreinte réelle → densité correcte.
    """
    if not PU_ZIPS:
        print("  Aucun Role_*_PU.zip – indicateurs PU ignorés."); return
    TYPE_MAP_IND={1:"Maisons individuelles détachées",2:"Maisons jumelées ou en rangée",
                  3:"Maisons jumelées ou en rangée",4:"Maisons jumelées ou en rangée",
                  5:"Immeuble comportant deux logements ou plus"}
    for annee_pu,zip_path in sorted(PU_ZIPS.items(),reverse=True):
        print(f"\n══ Indicateurs PU {annee_pu} ({zip_path.name}) ══")
        if not zip_path.exists(): continue
        try:
            with zipfile.ZipFile(zip_path) as zf:
                csv_e=next((n for n in zf.namelist() if n.endswith(".csv")),None)
                if not csv_e: continue
                print(f"  Lecture de {csv_e}…",flush=True)
                with zf.open(csv_e) as raw:
                    df=pd.read_csv(raw,low_memory=False,usecols=lambda c: c in [
                        "code_mun_2","rl0105a","rl0302a","rl0307a","rl0309a","rl0310a",
                        "rl0311a","rl0402a","rl0403a","rl0404a","CDNAME","mat18_2"])
            print(f"  {len(df):,} lignes totales")
            all_mrcs={v[1] for v in pf_lookup.values()}
            df=df[df["CDNAME"].isin(all_mrcs)].copy()
            print(f"  {len(df):,} lignes pour nos MRC")
            num_cols=["rl0302a","rl0307a","rl0309a","rl0310a","rl0311a","rl0402a","rl0403a","rl0404a"]
            df[num_cols]=df[num_cols].apply(pd.to_numeric,errors="coerce")
            df["rl0105a"]=df["rl0105a"].astype(str)
            df_res=df[df["rl0105a"].str.startswith("1")].copy()
            code_to_mun={code:nom for code,(nom,_) in pf_lookup.items()}
            df_res["CSDNAME"]=df_res["code_mun_2"].astype(str).str.zfill(5).map(code_to_mun)
            # Plan complementaire fix: sum fractional terrain shares per building
            df_res["plan_comp_id"]=df_res["mat18_2"].astype(str).str.zfill(18).str[:15]
            df_res["is_condo"]=df_res["rl0310a"]==5
            condo=df_res[df_res["is_condo"]].copy()
            non_condo=df_res[~df_res["is_condo"]].copy()
            if len(condo)>0:
                condo=condo.groupby(
                    ["plan_comp_id","CDNAME","CSDNAME","rl0307a","rl0309a","rl0105a"],dropna=False
                ).agg(rl0302a=("rl0302a","sum"),rl0311a=("rl0311a","sum"),
                      rl0402a=("rl0402a","mean"),rl0403a=("rl0403a","mean"),
                      rl0404a=("rl0404a","mean")).reset_index()
                df_res=pd.concat([non_condo,condo],ignore_index=True)
                print(f"  Après dissolution plans complémentaires : {len(df_res):,} lignes")
            df_new=df_res[(df_res["rl0307a"]>=2012)&(df_res["rl0307a"]<=annee_pu)].copy()
            def classify_ind(row):
                lp,ll=row["rl0309a"],row["rl0311a"]
                if pd.isna(lp) or pd.isna(ll): return "Autres logements"
                if ll>=2: return "Immeuble comportant deux logements ou plus"
                return TYPE_MAP_IND.get(int(lp),"Autres logements")
            df_new["Types"]=df_new.apply(classify_ind,axis=1)
            save_json(df_new.groupby(["CDNAME","rl0307a"]).agg(logements_PU=("rl0311a","sum")).reset_index().rename(columns={"rl0307a":"Annee_construction"}).round(1),"nouveaux_logements_mrc.json")
            save_json(df_new.groupby(["CDNAME","CSDNAME","rl0307a"]).agg(logements_PU=("rl0311a","sum")).reset_index().rename(columns={"rl0307a":"Annee_construction"}).round(1),"nouveaux_logements_mun.json")
            save_json(df_new.groupby(["CDNAME","rl0307a","Types"]).agg(logements=("rl0311a","sum")).reset_index().rename(columns={"rl0307a":"Annee_construction"}).round(1),"types_nouveaux_mrc.json")
            save_json(df_new.groupby(["CDNAME","CSDNAME","rl0307a","Types"]).agg(logements=("rl0311a","sum")).reset_index().rename(columns={"rl0307a":"Annee_construction"}).round(1),"types_nouveaux_mun.json")
            df_den=df_res[df_res["rl0309a"].notna()&(df_res["rl0309a"]!=0)].copy()
            df_den["terrain_ha"]=df_den["rl0302a"]/10000
            den_mrc=df_den.groupby(["CDNAME","rl0307a"]).agg(area_ha=("terrain_ha","sum"),units=("rl0309a","sum")).reset_index().rename(columns={"rl0307a":"Annee_construction"})
            den_mrc=den_mrc.sort_values(["CDNAME","Annee_construction"])
            den_mrc["cum_area"]=den_mrc.groupby("CDNAME")["area_ha"].cumsum()
            den_mrc["cum_units"]=den_mrc.groupby("CDNAME")["units"].cumsum()
            den_mrc["densite_nette_PU"]=np.where(den_mrc["cum_area"]>0,(den_mrc["cum_units"]/den_mrc["cum_area"]).round(3),np.nan)
            den_mrc=den_mrc[(den_mrc["Annee_construction"]>=2012)&(den_mrc["Annee_construction"]<=annee_pu)]
            save_json(den_mrc.round(3),"densite_pu_mrc.json")
            den_mun=df_den.groupby(["CDNAME","CSDNAME","rl0307a"]).agg(area_ha=("terrain_ha","sum"),units=("rl0309a","sum")).reset_index().rename(columns={"rl0307a":"Annee_construction"})
            den_mun=den_mun.sort_values(["CDNAME","CSDNAME","Annee_construction"])
            den_mun["cum_area"]=den_mun.groupby(["CDNAME","CSDNAME"])["area_ha"].cumsum()
            den_mun["cum_units"]=den_mun.groupby(["CDNAME","CSDNAME"])["units"].cumsum()
            den_mun["densite_nette_PU"]=np.where(den_mun["cum_area"]>0,(den_mun["cum_units"]/den_mun["cum_area"]).round(3),np.nan)
            den_mun=den_mun[(den_mun["Annee_construction"]>=2012)&(den_mun["Annee_construction"]<=annee_pu)]
            save_json(den_mun.round(3),"densite_pu_mun.json")
            break
        except Exception as e:
            print(f"  ERR PU {annee_pu}: {e}")
            import traceback; traceback.print_exc()
def prepare_cubf(df):
    d=df.copy()
    d["rl0105_str"]=d["rl0105a"].fillna("").astype(str).str.strip()
    d["rl0105_num"]=pd.to_numeric(d["rl0105_str"],errors="coerce")
    return d
def mamh_base_mask(d):
    rl=d["rl0105_num"]
    return rl.isin([1000,1010,1211])|((rl>=5000)&(rl<=5999))
def mamh_optional_mask(d):
    rl=d["rl0105_num"]
    return rl.isin([1543,1549])|((rl>=8100)&(rl<=8199))
def mamh_eligible_mask(d,include_optional=False):
    m=mamh_base_mask(d)
    if include_optional:
        m=m|mamh_optional_mask(d)
    return m
def assign_mamh_types(d,include_optional=False,colname="Types"):
    out=d.copy()
    out[colname]=None
    eligible=mamh_eligible_mask(out,include_optional=include_optional)
    out.loc[
        eligible&(out["rl0309a"]==1)&(out["rl0311a"]==1),
        colname
    ]="Maisons individuelles détachées"
    out.loc[
        eligible&out["rl0309a"].isin([2,3,4])&(out["rl0311a"]==1),
        colname
    ]="Maisons jumelées ou en rangée"
    out.loc[
        eligible&((((out["rl0309a"]==5)&(out["rl0311a"]==1))|(out["rl0311a"]>=2))),
        colname
    ]="Logements dans un immeuble comportant deux logements ou plus"
    return out
def assign_other_types_from_cubf(d,colname="Types"):
    out=d.copy()
    s=out["rl0105_str"]
    out[colname]=None
    out.loc[s.str.startswith("11"),colname]="Chalet et maison de villégiature"
    out.loc[s.str.startswith("12"),colname]="Maison mobile et roulotte"
    out.loc[s.str.startswith("15"),colname]="Habitation en commun"
    out.loc[s.str.startswith(tuple(["16","17","18","19"])),colname]="Autres immeubles résidentiels"
    return out
def build_role_universe(df,mode="mamh_strict"):
    d=prepare_cubf(df)
    if mode=="mamh_strict":
        d=assign_mamh_types(d,include_optional=False,colname="Types")
        return d.dropna(subset=["Types"]).copy()
    if mode=="mamh_optional":
        d=assign_mamh_types(d,include_optional=True,colname="Types")
        return d.dropna(subset=["Types"]).copy()
    if mode=="mamh_plus_others":
        d=assign_mamh_types(d,include_optional=True,colname="Types")
        mask_other=d["Types"].isna()
        if mask_other.any():
            d_other=assign_other_types_from_cubf(d.loc[mask_other].copy(),colname="Types")
            d.loc[mask_other,"Types"]=d_other["Types"]
        return d.dropna(subset=["Types"]).copy()
    raise ValueError(f"Mode inconnu: {mode}")
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
def export_indicator_set(Role_brut, mode, suffix):
    Role_UE=build_role_universe(Role_brut,mode=mode)
    Role_exp=expand_logements(Role_UE)
    print(f"  [{suffix}] {len(Role_UE):,} UE / {len(Role_exp):,} logements")
    mrc_types=(Role_exp.groupby(["Annee","CDNAME","Types"]).agg(N=("Annee","count")).reset_index()
        .pivot_table(index=["Annee","CDNAME"],columns="Types",values="N",aggfunc="sum").reset_index())
    mrc_types.columns.name=None
    for col in TYPE_COLS:
        if col not in mrc_types.columns: mrc_types[col]=0
    mrc_types["Total"]=mrc_types[TYPE_COLS].sum(axis=1)
    for col in TYPE_COLS:
        mrc_types[f"{col}_pct"]=np.where(mrc_types["Total"]>0,(mrc_types[col]/mrc_types["Total"]*100).round(2),np.nan)
    save_json(mrc_types,f"logements_types_mrc_{suffix}.json")
    save_json(Role_exp.groupby(["Annee","CDNAME","CSDNAME","Types"]).agg(N=("Annee","count")).reset_index().rename(columns={"Types":"Types de construction résidentielle","N":"Nombre de logements"}),f"logements_types_mun_{suffix}.json")
    mrc_val=Role_UE.groupby(["Annee","CDNAME","Types"]).agg(terrain=("rl0402a","mean"),batiment=("rl0403a","mean"),immeuble=("rl0404a","mean")).reset_index()
    tot=Role_UE.groupby(["Annee","CDNAME"]).agg(terrain=("rl0402a","mean"),batiment=("rl0403a","mean"),immeuble=("rl0404a","mean")).reset_index(); tot["Types"]="Total des unités d'évaluation résidentielles"
    save_json(pd.concat([mrc_val,tot],ignore_index=True).round(0),f"valeur_mrc_{suffix}.json")
    save_json(Role_UE.groupby(["Annee","CDNAME","CSDNAME","Types"]).agg(terrain=("rl0402a","mean"),batiment=("rl0403a","mean"),immeuble=("rl0404a","mean")).reset_index().round(0),f"valeur_mun_{suffix}.json")
    mrc_age=Role_UE.groupby(["Annee","CDNAME","Types"]).agg(annee_moy=("rl0307a","mean")).reset_index()
    tot_age=Role_UE.groupby(["Annee","CDNAME"]).agg(annee_moy=("rl0307a","mean")).reset_index(); tot_age["Types"]="Total des unités d'évaluation résidentielles"
    mrc_age=pd.concat([mrc_age,tot_age],ignore_index=True); mrc_age["age_moyen"]=(mrc_age["Annee"]-mrc_age["annee_moy"]).round(1)
    save_json(mrc_age,f"age_mrc_{suffix}.json")
    mun_age=Role_UE.groupby(["Annee","CDNAME","CSDNAME","Types"]).agg(annee_moy=("rl0307a","mean")).reset_index()
    tot_age_mun=Role_UE.groupby(["Annee","CDNAME","CSDNAME"]).agg(annee_moy=("rl0307a","mean")).reset_index(); tot_age_mun["Types"]="Total des unités d'évaluation résidentielles"
    mun_age=pd.concat([mun_age,tot_age_mun],ignore_index=True); mun_age["age_moyen"]=(mun_age["Annee"]-mun_age["annee_moy"]).round(1)
    save_json(mun_age,f"age_mun_{suffix}.json")
    Role_UE_per=Role_UE.copy(); Role_UE_per["Période"]=Role_UE_per["rl0307a"].apply(categorize_periode)
    save_json(Role_UE_per.dropna(subset=["Période"]).groupby(["Annee","CDNAME","Types","Période"]).agg(N=("Annee","count")).reset_index(),f"periode_mrc_{suffix}.json")
    save_json(Role_UE_per.dropna(subset=["Période"]).groupby(["Annee","CDNAME","CSDNAME","Types","Période"]).agg(N=("Annee","count")).reset_index(),f"periode_mun_{suffix}.json")
    mrc_sup=Role_UE.groupby(["Annee","CDNAME","Types"]).agg(superficie_terrain=("rl0302a","mean"),aire_etages=("rl0308a","mean")).reset_index()
    tot_sup=Role_UE.groupby(["Annee","CDNAME"]).agg(superficie_terrain=("rl0302a","mean"),aire_etages=("rl0308a","mean")).reset_index(); tot_sup["Types"]="Total des unités d'évaluation résidentielles"
    save_json(pd.concat([mrc_sup,tot_sup],ignore_index=True).round(1),f"superficie_mrc_{suffix}.json")
    mun_sup=Role_UE.groupby(["Annee","CDNAME","CSDNAME","Types"]).agg(superficie_terrain=("rl0302a","mean"),aire_etages=("rl0308a","mean")).reset_index()
    tot_sup_mun=Role_UE.groupby(["Annee","CDNAME","CSDNAME"]).agg(superficie_terrain=("rl0302a","mean"),aire_etages=("rl0308a","mean")).reset_index(); tot_sup_mun["Types"]="Total des unités d'évaluation résidentielles"
    save_json(pd.concat([mun_sup,tot_sup_mun],ignore_index=True).round(1),f"superficie_mun_{suffix}.json")
def main():
    MATCH=load_match(); our_mrcs=set(MATCH["CDNAME"].unique())
    pf_lookup=load_pf_mun(our_mrcs)
    print(f"\nSHP zips : {dict(sorted(SHP_ZIPS.items())) or 'aucun'}")
    print(f"PU  zips : {dict(sorted(PU_ZIPS.items())) or 'aucun'}")
    rows_api,_=build_role_df_api(MATCH)
    rows_shp,_=build_role_df_shp(pf_lookup,MATCH)
    all_rows=rows_api+rows_shp
    if not all_rows: print("Aucune donnée."); return
    Role_brut=pd.DataFrame(all_rows)
    num_cols=["rl0302a","rl0307a","rl0308a","rl0309a","rl0311a","rl0402a","rl0403a","rl0404a"]
    Role_brut[num_cols]=Role_brut[num_cols].apply(pd.to_numeric,errors="coerce")
    Role_brut["rl0105a"]=Role_brut["rl0105a"].fillna("").astype(str)
    annees=sorted(Role_brut["Annee"].unique())
    print(f"\n✓ Brut : {len(Role_brut):,} UE sur {len(annees)} années ({annees})")
    print("\nExport des indicateurs par mode de catégorisation...")
    export_indicator_set(Role_brut,mode="mamh_strict",suffix="mamh_strict")
    export_indicator_set(Role_brut,mode="mamh_optional",suffix="mamh_optional")
    export_indicator_set(Role_brut,mode="mamh_plus_others",suffix="mamh_plus_others")
    mrc_list=MATCH.groupby("CDNAME")["Municipalité"].apply(list).reset_index(); mrc_list.columns=["CDNAME","municipalites"]
    region_map=MATCH.drop_duplicates("CDNAME").set_index("CDNAME")["Region"].to_dict(); mrc_list["Region"]=mrc_list["CDNAME"].map(region_map)
    save_json(mrc_list,"mrc_list.json")
    build_indicateurs_pu(pf_lookup)
    print("\n🎉 Pipeline terminé.")
if __name__=="__main__":
    main()
