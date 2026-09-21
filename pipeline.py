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
import io, time, csv, json, os, zipfile, re, struct, shutil
from collections import Counter
import numpy as np
from datetime import date
from pathlib import Path
POP_HIST_PATH     = "pop-hist-mrc.xlsx"
POP_PROJ_PATH     = "pop-proj-mrc.xlsx"
MENAGES_PROJ_PATH = "menages-proj-mrc.xlsx"
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
QA_DROPPED_YEARS = []
MATCH_PATH  = "MATCH.csv"
PF_MUN_PATH = "pf-mun-2023-2023.csv"
DATA_DIR    = Path("web/data")
DATA_DIR.mkdir(exist_ok=True)
# Détail municipal, éclaté par région administrative (voir save_json_mun).
MUN_DIR     = DATA_DIR/"mun"
MODES       = ["mamh_strict","mamh_optional","mamh_plus_others"]
# Un territoire équivalent à une MRC est écrit « Hors MRC - X » au profil financier ;
# trois d'entre eux portent un autre nom chez l'ISQ (population, ménages, projections).
# MATCH.csv est construit avec les mêmes règles : les deux doivent rester alignés,
# sans quoi les MRC du rôle ne rejoignent plus celles des données de population.
MRC_ALIASES = {
    "Des Chenaux": "Les Chenaux",
    "Les Îles de la Madeleine": "Communauté maritime des Îles-de-la-Madeleine",
    "Nord du Québec": "Jamésie",
}
def normalize_mrc(nom):
    nom=nom.strip()
    if nom.startswith("Hors MRC - "): nom=nom[len("Hors MRC - "):].strip()
    return MRC_ALIASES.get(nom,nom)
TYPE_COLS = [
    "Maisons individuelles détachées",
    "Maisons jumelées ou en rangée",
    "Logements dans un immeuble comportant deux logements ou plus",
    "Chalet et maison de villégiature",
    "Habitation en commun",
    "Maison mobile et roulotte",
    "Autres immeubles résidentiels",
]
# Type écarté de l'onglet Superficie (voir indicator_frames) et libellé du total qui en découle.
SUP_TYPE_EXCLU = "Autres immeubles résidentiels"
SUP_TOTAL_LABEL = "Total des types de construction présentés"
def load_match():
    df = pd.read_csv(MATCH_PATH)
    if len(df.columns)==2: df.columns=["Municipalité","CDNAME"]; df["Region"]=None
    else: df.columns=["Municipalité","CDNAME","Region"]
    df["_mun_key"]=df["Municipalité"].str.strip()
    print(f"MATCH: {len(df)} municipalités, {df['CDNAME'].nunique()} MRC")
    return df
def load_pf_mun(our_mrcs):
    """Codes géographiques → (municipalité, MRC), plus le nom des régions administratives.

    Le code géographique, et non le nom, identifie une municipalité : à l'échelle du
    Québec trente-deux noms sont portés par deux municipalités de MRC différentes, et
    dix le sont deux fois dans une même MRC (Bedford V et Bedford CT, Hatley M et
    Hatley CT…). Ces dix paires reçoivent le suffixe de désignation employé par le
    MAMH et l'ISQ, faute de quoi leurs données fusionneraient dans les tableaux.
    """
    p=Path(PF_MUN_PATH)
    if not p.exists(): print(f"  ⚠  {PF_MUN_PATH} introuvable"); return {},{}
    raw=[]; regions={}
    with open(p,encoding="utf-8-sig",newline="") as f:
        for row in csv.DictReader(f):
            code=row["cod_geo_n"].strip()
            if not (code.isdigit() and len(code)==5): continue
            nom_mun=row["nom_mun"].strip(); nom_mrc=normalize_mrc(row["nom_mrc"])
            if nom_mun=="Clarenceville" and nom_mrc=="Le Haut-Richelieu":
                nom_mun="Saint-Georges-de-Clarenceville"
            cod_ra=row["cod_ra"].strip()
            if cod_ra.isdigit(): regions[int(cod_ra)]=row["nom_ra"].strip()
            raw.append((code,nom_mun,nom_mrc,row["designation"].strip()))
    homonymes={k for k,n in Counter((m,mrc) for _,m,mrc,_ in raw).items() if n>1}
    lookup={}
    for code,nom_mun,nom_mrc,designation in raw:
        if (nom_mun,nom_mrc) in homonymes: nom_mun=f"{nom_mun} ({designation})"
        if nom_mrc in our_mrcs: lookup[code]=(nom_mun,nom_mrc)
    print(f"pf-mun : {len(lookup)} codes pour nos {len(our_mrcs)} MRC, {len(regions)} régions")
    return lookup,regions
RAW_NUM_COLS=["rl0302a","rl0307a","rl0308a","rl0309a","rl0311a","rl0402a","rl0403a","rl0404a"]
RAW_COLS=["CSDNAME","CDNAME","rl0105a"]+RAW_NUM_COLS
def compact_frame(records,annee):
    """Lot d'unités d'évaluation converti tout de suite, au lieu d'être accumulé en texte.

    Le pipeline couvre les 1 100 municipalités du Québec : une année du rôle
    provincial approche les quatre millions d'unités d'évaluation. Gardées en
    chaînes de caractères — une par champ et par unité — ces colonnes épuisent la
    mémoire d'un runner GitHub Actions ; converties, elles tiennent dans quelques
    centaines de Mo.

    Les mesures restent en flottants 64 bits : les moyennes exportées (âge moyen,
    valeurs foncières) portent sur des milliers d'unités, et la précision d'un
    float32 s'y perdrait à la première décimale.

    Le code d'utilisation des biens-fonds devient une catégorie : il ne sert jamais
    de clé de regroupement, et le stocker une fois pour toutes évite des millions de
    chaînes distinctes. CSDNAME et CDNAME, eux, restent en texte — ce sont des objets
    partagés (un par municipalité, un par MRC), donc déjà peu coûteux, alors qu'une
    clé de regroupement catégorielle ferait produire à pandas le produit cartésien
    de toutes les catégories à chaque agrégation.
    """
    df=pd.DataFrame(records,columns=RAW_COLS)
    df["Annee"]=np.int32(annee)
    for c in RAW_NUM_COLS:
        df[c]=pd.to_numeric(df[c],errors="coerce")
    # Nettoyé une fois pour toutes ici : prepare_cubf lit ensuite la colonne telle quelle.
    df["rl0105a"]=df["rl0105a"].fillna("").astype(str).str.strip().astype("category")
    return df
def parse_xml(content,annee,nom_mun,cdname):
    root=ET.fromstring(content); records=[]
    for ue in root.findall("RLUEx"):
        def g(tag,_ue=ue):
            v=_ue.findtext(tag); return v.strip() if v else None
        records.append((nom_mun,cdname,g("RL0105A"),g("RL0302A"),g("RL0307A"),
            g("RL0308A"),g("RL0309A"),g("RL0311A"),
            g("RL0402A"),g("RL0403A"),g("RL0404A")))
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
    col_code=next((c for c in cols if "code" in c.lower()),None)
    print(f"{len(rows)} muns")
    return [(row[col_code].strip() if col_code else "", row[col_nom].strip(), row[col_url].strip())
            for row in rows if row.get(col_nom) and row.get(col_url)]
def fetch_role_year_api(annee,match_df,pf_lookup,pause=0.1):
    """Une année du rôle via l'API MAMH, renvoyée en lots déjà compactés.

    L'index est parcouru par code géographique : c'est lui, et non le nom, qui
    identifie une municipalité (voir load_pf_mun). Les organismes sans code au
    profil financier — territoires non organisés, réserves — restent rattachés
    par leur nom, tel que MATCH.csv le donne.
    """
    mun_to_mrc=match_df.drop_duplicates("_mun_key").set_index("_mun_key")["CDNAME"].to_dict()
    index=fetch_index(annee)
    if not index: return [],[]
    frames,errors=[],[]; n_mun=0
    for code,nom_mun,url_xml in index:
        est_code=bool(re.match(r'^\d{5}$',code))
        if est_code:
            if code not in pf_lookup: continue
            nom_ref,cdname=pf_lookup[code]
        else:
            if nom_mun not in mun_to_mrc: continue
            nom_ref,cdname=nom_mun,mun_to_mrc[nom_mun]
        # Correction préventive d'une URL d'index erronée : le fichier attendu est RL{code}_{annee}.xml
        if est_code:
            base_url=url_xml.rsplit('/',1)[0]
            expected=f"RL{code}_{annee}.xml"
            if url_xml.rsplit('/',1)[-1]!=expected:
                print(f"  ⚠ URL index incorrecte ({url_xml.rsplit('/',1)[-1]}) → {expected}")
                url_xml=f"{base_url}/{expected}"
        print(f"  [{annee}] {nom_ref} ({url_xml.split('/')[-1]})…",end=" ",flush=True)
        try:
            r=requests.get(url_xml,timeout=60); r.raise_for_status()
            rows=parse_xml(r.content,annee,nom_ref,cdname)
            frames.append(compact_frame(rows,annee)); n_mun+=1; print(f"{len(rows):,} UE")
        except Exception as e:
            if est_code:
                base_url=url_xml.rsplit('/',1)[0]
                url_fallback=f"{base_url}/RL{code}_{annee}.xml"
                print(f"↻ fallback RL{code}…",end=" ",flush=True)
                try:
                    r=requests.get(url_fallback,timeout=60); r.raise_for_status()
                    rows=parse_xml(r.content,annee,nom_ref,cdname)
                    frames.append(compact_frame(rows,annee)); n_mun+=1; print(f"{len(rows):,} UE")
                except Exception as e2:
                    print(f"ERR: {e2}"); errors.append({"annee":annee,"mun":nom_ref,"erreur":str(e2)})
            else:
                print(f"ERR: {e}"); errors.append({"annee":annee,"mun":nom_ref,"erreur":str(e)})
        time.sleep(pause)
    print(f"  ✓ {annee} : {n_mun} municipalités")
    return frames,errors
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
def read_role_year_shp(annee,zip_path,pf_lookup):
    """Une année du rôle depuis le SHP provincial, lue par lots compactés au fil de l'eau.

    Le DBF provincial porte près de quatre millions d'enregistrements : ils sont
    convertis tous les CHUNK enregistrements plutôt qu'accumulés en mémoire vive.
    """
    CHUNK=250_000
    print(f"\n══ Année {annee} (SHP : {zip_path.name}) ══")
    if not zip_path.exists(): print(f"  ⚠  Zip introuvable"); return [],[]
    try:
        with zipfile.ZipFile(zip_path,"r") as zf:
            def is_main_dbf(name):
                u=name.upper()
                if "ADR_UNITE_EVALN" in u: return False
                return (u.endswith("B05EX1_B05V_UNITE_EVALN.DBF") or
                        u.endswith("B05V_UNITE_EVALN.DBF") or
                        u.endswith("UNITE_EVALN.DBF"))
            dbf_entry=next((n for n in zf.namelist() if is_main_dbf(n)),None)
            if not dbf_entry: print(f"  ⚠  DBF introuvable"); return [],[]
            print(f"  Décompression de {dbf_entry}…",flush=True)
            with zf.open(dbf_entry) as raw:
                import io as _io; data=_io.BytesIO(raw.read())
        num_records,header_size,record_size,offsets=_read_dbf_layout(data)
        print(f"  {num_records:,} enregistrements")
        code_pos,code_len=offsets["code_mun"]
        frames=[]; buf=[]; rows_year=0; found_codes=set()
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
            buf.append((nom_mun,cdname,get("rl0105a"),get("rl0302a"),get("rl0307a"),
                get("rl0308a"),get("rl0309a"),get("rl0311a"),
                get("rl0402a"),get("rl0403a"),get("rl0404a")))
            rows_year+=1
            if len(buf)>=CHUNK:
                frames.append(compact_frame(buf,annee)); buf=[]
        if buf: frames.append(compact_frame(buf,annee))
        # Contrôle de vraisemblance : un zip tronqué ou rejeté livre une fraction
        # des unités attendues. Le seuil suit le périmètre du pipeline (≈ 800 unités
        # par municipalité couverte) au lieu de la valeur absolue calibrée du temps
        # où il ne portait que trois régions.
        seuil=800*max(len(pf_lookup),1)
        if rows_year<seuil:
            print(f"  ⚠  {rows_year:,} UE (seuil {seuil:,}) – zip suspect, ignoré.")
            QA_DROPPED_YEARS.append({"annee":annee,"raison":f"SHP suspect ({rows_year:,} UE < {seuil:,})"})
            return [],[]
        print(f"  ✓ {rows_year:,} UE retenues ({len(found_codes)} municipalités)")
        return frames,[]
    except Exception as e:
        print(f"  ERR SHP {annee}: {e}")
        return [],[{"annee":annee,"mun":"SHP","erreur":str(e)}]
def year_batches(match_df,pf_lookup):
    """Livre le rôle année par année : chaque année est agrégée puis libérée.

    Tenir toutes les années en mémoire simultanément était possible sur trois
    régions ; sur le Québec entier cela représenterait des dizaines de millions
    d'unités d'évaluation. Comme chaque indicateur exporté est ventilé par année,
    agréger année par année donne exactement les mêmes fichiers.
    """
    for annee in [a for a in ANNEES if a>=2023 and a not in SHP_ZIPS]:
        print(f"\n══ Année {annee} (API) ══")
        frames,_=fetch_role_year_api(annee,match_df,pf_lookup)
        if frames: yield annee,frames
    for annee,zip_path in sorted(SHP_ZIPS.items()):
        frames,_=read_role_year_shp(annee,zip_path,pf_lookup)
        if frames: yield annee,frames
def build_indicateurs_pu(pf_lookup,region_by_mrc):
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
            save_json_mun(df_new.groupby(["CDNAME","CSDNAME","rl0307a"]).agg(logements_PU=("rl0311a","sum")).reset_index().rename(columns={"rl0307a":"Annee_construction"}).round(1),"nouveaux_logements_mun",region_by_mrc)
            save_json(df_new.groupby(["CDNAME","rl0307a","Types"]).agg(logements=("rl0311a","sum")).reset_index().rename(columns={"rl0307a":"Annee_construction"}).round(1),"types_nouveaux_mrc.json")
            save_json_mun(df_new.groupby(["CDNAME","CSDNAME","rl0307a","Types"]).agg(logements=("rl0311a","sum")).reset_index().rename(columns={"rl0307a":"Annee_construction"}).round(1),"types_nouveaux_mun",region_by_mrc)
            df_den=df_res[df_res["rl0309a"].notna()&(df_res["rl0309a"]!=0)].copy()
            df_den["terrain_ha"]=df_den["rl0302a"]/10000
            den_mrc=df_den.groupby(["CDNAME","rl0307a"]).agg(area_ha=("terrain_ha","sum"),units=("rl0311a","sum")).reset_index().rename(columns={"rl0307a":"Annee_construction"})
            den_mrc=den_mrc.sort_values(["CDNAME","Annee_construction"])
            den_mrc["cum_area"]=den_mrc.groupby("CDNAME")["area_ha"].cumsum()
            den_mrc["cum_units"]=den_mrc.groupby("CDNAME")["units"].cumsum()
            den_mrc["densite_nette_PU"]=np.where(den_mrc["cum_area"]>0,(den_mrc["cum_units"]/den_mrc["cum_area"]).round(3),np.nan)
            den_mrc=den_mrc[(den_mrc["Annee_construction"]>=2012)&(den_mrc["Annee_construction"]<=annee_pu)]
            save_json(den_mrc.round(3),"densite_pu_mrc.json")
            den_mun=df_den.groupby(["CDNAME","CSDNAME","rl0307a"]).agg(area_ha=("terrain_ha","sum"),units=("rl0311a","sum")).reset_index().rename(columns={"rl0307a":"Annee_construction"})
            den_mun=den_mun.sort_values(["CDNAME","CSDNAME","Annee_construction"])
            den_mun["cum_area"]=den_mun.groupby(["CDNAME","CSDNAME"])["area_ha"].cumsum()
            den_mun["cum_units"]=den_mun.groupby(["CDNAME","CSDNAME"])["units"].cumsum()
            den_mun["densite_nette_PU"]=np.where(den_mun["cum_area"]>0,(den_mun["cum_units"]/den_mun["cum_area"]).round(3),np.nan)
            den_mun=den_mun[(den_mun["Annee_construction"]>=2012)&(den_mun["Annee_construction"]<=annee_pu)]
            save_json_mun(den_mun.round(3),"densite_pu_mun",region_by_mrc)
            break
        except Exception as e:
            print(f"  ERR PU {annee_pu}: {e}")
            import traceback; traceback.print_exc()
def prepare_cubf(df):
    d=df.copy()
    # compact_frame livre la colonne déjà nettoyée : la repasser en texte suffit.
    d["rl0105_str"]=d["rl0105a"].astype(str)
    d["rl0105_num"]=pd.to_numeric(d["rl0105_str"],errors="coerce")
    return d
def mamh_base_mask(d):
    # Fiche méthodologique OGAT (indicateur stratégique habitation), sources de données :
    # utilisation prédominante « résidentielle » ou « commerciale » = 1000, 1010, 1211, 1702, 5000-5999.
    rl=d["rl0105_num"]
    return rl.isin([1000,1010,1211,1702])|((rl>=5000)&(rl<=5999))
def mamh_optional_mask(d):
    # Fiche méthodologique OGAT, attributs facultatifs : « les codes 1543, 1549 et ceux
    # compris entre 8120 et 8199, si la MRC dispose de données précises à cet égard ».
    # La plage part de 8120 : 8100 est l'en-tête générique de la classe agricole du CUBF
    # et 8110 n'est pas retenu par la fiche.
    rl=d["rl0105_num"]
    return rl.isin([1543,1549])|((rl>=8120)&(rl<=8199))
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
    # Unité éligible CUBF, rl0311a==1, mais rl0309a non reconnu → Autres immeubles résidentiels
    mask_autres = eligible & out[colname].isna() & (out["rl0311a"]==1)
    out.loc[mask_autres, colname]="Autres immeubles résidentiels"
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
        eligible=mamh_eligible_mask(d,include_optional=True)
        # Le repli par préfixe CUBF ne vaut que pour les codes hors liste MAMH. Pour un code
        # éligible, ce sont les règles MAMH qui font autorité : l'absence de type traduit un
        # nombre de logements manquant au rôle, pas une autre famille de bâtiment. Sans cette
        # restriction, une unité 1211 (immeuble multifamilial) sans nombre de logements tombait
        # sur le préfixe « 12 » et était classée « Maison mobile et roulotte ».
        mask_other=d["Types"].isna() & ~eligible
        if mask_other.any():
            d_other=assign_other_types_from_cubf(d.loc[mask_other].copy(),colname="Types")
            d.loc[mask_other,"Types"]=d_other["Types"]
        # Unité éligible mais non typable : conservée dans le portrait exhaustif du filtre
        # complet, sous la catégorie déjà employée pour les unités éligibles dont le lien
        # physique n'est pas reconnu.
        d.loc[d["Types"].isna()&eligible,"Types"]="Autres immeubles résidentiels"
        return d.dropna(subset=["Types"]).copy()
    raise ValueError(f"Mode inconnu: {mode}")
def compter_logements(Role_UE,keys):
    """Nombre de logements par groupe.

    Donne exactement le résultat de l'ancienne expansion — répéter chaque unité
    d'évaluation autant de fois qu'elle compte de logements, puis compter les
    lignes — sans matérialiser cette expansion, qui dépasserait quatre millions
    de lignes par année à l'échelle du Québec. Comme dans l'expansion, les unités
    sans nombre de logements exploitable (valeur absente ou nulle) ne comptent pas.
    """
    d=Role_UE[Role_UE["rl0311a"].notna()]
    n=d["rl0311a"].astype("int64")
    garde=n>0
    d=d[garde].assign(_n=n[garde])
    return d.groupby(keys).agg(N=("_n","sum")).reset_index()
def categorize_periode(val):
    if pd.isna(val): return None
    elif val<=1960: return "1960 ou avant"
    elif val<=1980: return "1961-1980"
    elif val<=2000: return "1981-2000"
    elif val<=2015: return "2001-2015"
    else: return "2016 et plus"
def save_json(df,path):
    (DATA_DIR/path).parent.mkdir(parents=True,exist_ok=True)
    df.to_json(DATA_DIR/path,orient="records",force_ascii=False,indent=None)
    print(f"  ✓ {path} ({len(df):,} lignes)")
def save_json_mun(df,stem,region_by_mrc):
    """Écrit un indicateur municipal en un fichier par région administrative.

    Le détail municipal du Québec entier pèse plusieurs dizaines de Mo par
    indicateur. Le tableau de bord n'a jamais besoin que de la région consultée :
    il ne télécharge donc que le fragment correspondant.
    """
    reg=df["CDNAME"].map(region_by_mrc)
    inconnues=sorted(set(df.loc[reg.isna(),"CDNAME"]))
    if inconnues: print(f"  ⚠  {stem} : MRC sans région – {inconnues[:5]}")
    connu=df[reg.notna()]
    for r,part in connu.groupby(reg[reg.notna()].astype(int)):
        save_json(part,f"mun/{stem}_r{r}.json")
def superficie_frame(Role_UE,keys):
    """
    Superficies de terrain et aires d'étages, par unité d'évaluation et par logement.

    Les valeurs « par logement » sont un ratio des sommes (Σ superficie / Σ rl0311a) :
    chaque logement pèse également, conformément à la comptabilisation prévue par la
    fiche méthodologique OGAT. La division règle du même coup l'hétérogénéité de la
    catégorie « Logements dans un immeuble en comportant deux et plus » : une unité de
    condo porte déjà sa quote-part de terrain et l'aire de son seul logement (rl0311a=1,
    division sans effet), tandis qu'un plex porte le terrain et l'aire de tout l'immeuble
    (division par son nombre de logements). Pour les maisons détachées et jumelées,
    rl0311a vaut 1 : les valeurs par logement sont identiques aux moyennes par unité.

    Le dénominateur de chaque ratio est restreint aux unités dont le numérateur est
    renseigné, afin que numérateur et dénominateur portent sur les mêmes unités. Les
    unités sans nombre de logements exploitable (rl0311a nul ou absent, possible en
    mode « complet ») sont écartées des ratios.

    Une superficie nulle n'est pas une mesure : elle signale une donnée non relevée, et
    les deux sources l'écrivent différemment. Les XML de l'API laissent RL0308A vide,
    tandis que les DBF des SHP provinciaux (2012-2022) y inscrivent 0. Compter ces zéros
    comme des aires réelles écrasait les moyennes de la période SHP — l'aire d'étages
    moyenne des « Autres immeubles résidentiels », catégorie riche en unités agricoles
    ou sans lien physique reconnu, tombait ainsi à 1 m² par logement, et la rupture se
    voyait aussi sur les immeubles à logements multiples entre 2022 et 2023. Les valeurs
    non strictement positives sont donc traitées comme absentes, au numérateur comme au
    dénominateur, ce qui aligne les deux sources.

    Les colonnes n_log_* sont les dénominateurs employés : repondérer les moyennes par
    ces effectifs redonne exactement le ratio agrégé sur plusieurs territoires.
    """
    d=Role_UE.copy()
    log=pd.to_numeric(d["rl0311a"],errors="coerce").where(lambda s:s>0)
    terr=pd.to_numeric(d["rl0302a"],errors="coerce").where(lambda s:s>0)
    aire=pd.to_numeric(d["rl0308a"],errors="coerce").where(lambda s:s>0)
    d["_terr_ue"]=terr
    d["_aire_ue"]=aire
    d["_terr"]=terr.where(log.notna())
    d["_aire"]=aire.where(log.notna())
    d["_log_terr"]=log.where(terr.notna())
    d["_log_aire"]=log.where(aire.notna())
    g=d.groupby(keys).agg(
        superficie_terrain=("_terr_ue","mean"),
        aire_etages=("_aire_ue","mean"),
        n_ue=("Annee","size"),
        _terr=("_terr","sum"),_log_terr=("_log_terr","sum"),
        _aire=("_aire","sum"),_log_aire=("_log_aire","sum"),
    ).reset_index()
    g["superficie_terrain_par_log"]=np.where(g["_log_terr"]>0,g["_terr"]/g["_log_terr"],np.nan)
    g["aire_etages_par_log"]=np.where(g["_log_aire"]>0,g["_aire"]/g["_log_aire"],np.nan)
    g["n_log_terrain"]=g["_log_terr"]
    g["n_log_aire"]=g["_log_aire"]
    return g.drop(columns=["_terr","_log_terr","_aire","_log_aire"])
def indicator_frames(Role_brut, mode, suffix):
    """Agrège une année du rôle selon un mode de catégorisation CUBF.

    Renvoie {nom de fichier: (portée, tableau)} au lieu d'écrire directement :
    main() empile les tableaux année après année, puis écrit une seule fois. La
    portée « mun » désigne le détail municipal, éclaté par région à l'écriture.
    """
    Role_UE=build_role_universe(Role_brut,mode=mode)
    mrc_log=compter_logements(Role_UE,["Annee","CDNAME","Types"])
    mun_log=compter_logements(Role_UE,["Annee","CDNAME","CSDNAME","Types"])
    print(f"  [{suffix}] {len(Role_UE):,} UE / {int(mrc_log['N'].sum()):,} logements")
    mrc_types=(mrc_log.pivot_table(index=["Annee","CDNAME"],columns="Types",values="N",aggfunc="sum").reset_index())
    mrc_types.columns.name=None
    for col in TYPE_COLS:
        if col not in mrc_types.columns: mrc_types[col]=0
    mrc_types["Total"]=mrc_types[TYPE_COLS].sum(axis=1)
    for col in TYPE_COLS:
        mrc_types[f"{col}_pct"]=np.where(mrc_types["Total"]>0,(mrc_types[col]/mrc_types["Total"]*100).round(2),np.nan)
    out={}
    out[f"logements_types_mrc_{suffix}"]=("mrc",mrc_types)
    out[f"logements_types_mun_{suffix}"]=("mun",mun_log.rename(columns={"Types":"Types de construction résidentielle","N":"Nombre de logements"}))
    mrc_val=Role_UE.groupby(["Annee","CDNAME","Types"]).agg(terrain=("rl0402a","mean"),batiment=("rl0403a","mean"),immeuble=("rl0404a","mean"),n_ue=("Annee","size")).reset_index()
    tot=Role_UE.groupby(["Annee","CDNAME"]).agg(terrain=("rl0402a","mean"),batiment=("rl0403a","mean"),immeuble=("rl0404a","mean"),n_ue=("Annee","size")).reset_index(); tot["Types"]="Total des unités d'évaluation résidentielles"
    out[f"valeur_mrc_{suffix}"]=("mrc",pd.concat([mrc_val,tot],ignore_index=True).round(0))
    out[f"valeur_mun_{suffix}"]=("mun",Role_UE.groupby(["Annee","CDNAME","CSDNAME","Types"]).agg(terrain=("rl0402a","mean"),batiment=("rl0403a","mean"),immeuble=("rl0404a","mean"),n_ue=("Annee","size")).reset_index().round(0))
    mrc_age=Role_UE.groupby(["Annee","CDNAME","Types"]).agg(annee_moy=("rl0307a","mean"),n_ue=("Annee","size")).reset_index()
    tot_age=Role_UE.groupby(["Annee","CDNAME"]).agg(annee_moy=("rl0307a","mean"),n_ue=("Annee","size")).reset_index(); tot_age["Types"]="Total des unités d'évaluation résidentielles"
    mrc_age=pd.concat([mrc_age,tot_age],ignore_index=True); mrc_age["age_moyen"]=(mrc_age["Annee"]-mrc_age["annee_moy"]).round(1)
    out[f"age_mrc_{suffix}"]=("mrc",mrc_age)
    mun_age=Role_UE.groupby(["Annee","CDNAME","CSDNAME","Types"]).agg(annee_moy=("rl0307a","mean")).reset_index()
    tot_age_mun=Role_UE.groupby(["Annee","CDNAME","CSDNAME"]).agg(annee_moy=("rl0307a","mean")).reset_index(); tot_age_mun["Types"]="Total des unités d'évaluation résidentielles"
    mun_age=pd.concat([mun_age,tot_age_mun],ignore_index=True); mun_age["age_moyen"]=(mun_age["Annee"]-mun_age["annee_moy"]).round(1)
    out[f"age_mun_{suffix}"]=("mun",mun_age)
    Role_UE_per=Role_UE.copy(); Role_UE_per["Période"]=Role_UE_per["rl0307a"].apply(categorize_periode)
    Role_UE_per=Role_UE_per.dropna(subset=["Période"])
    out[f"periode_mrc_{suffix}"]=("mrc",Role_UE_per.groupby(["Annee","CDNAME","Types","Période"]).agg(N=("Annee","count")).reset_index())
    out[f"periode_mun_{suffix}"]=("mun",Role_UE_per.groupby(["Annee","CDNAME","CSDNAME","Types","Période"]).agg(N=("Annee","count")).reset_index())
    # « Autres immeubles résidentiels » regroupe les unités résidentielles dont le lien
    # physique n'est pas reconnu au rôle : exploitations agricoles, hôtels et résidences
    # provisoires, unités sans nombre de logements exploitable. Leur terrain se compte en
    # dizaines d'hectares par logement — il décrit une propriété foncière, pas la superficie
    # d'une habitation — et dominait le total de la MRC. La catégorie est écartée des
    # superficies ; le total porte donc sur les seuls types présentés.
    Role_sup=Role_UE[Role_UE["Types"]!=SUP_TYPE_EXCLU]
    mrc_sup=superficie_frame(Role_sup,["Annee","CDNAME","Types"])
    tot_sup=superficie_frame(Role_sup,["Annee","CDNAME"]); tot_sup["Types"]=SUP_TOTAL_LABEL
    out[f"superficie_mrc_{suffix}"]=("mrc",pd.concat([mrc_sup,tot_sup],ignore_index=True).round(1))
    mun_sup=superficie_frame(Role_sup,["Annee","CDNAME","CSDNAME","Types"])
    tot_sup_mun=superficie_frame(Role_sup,["Annee","CDNAME","CSDNAME"]); tot_sup_mun["Types"]=SUP_TOTAL_LABEL
    out[f"superficie_mun_{suffix}"]=("mun",pd.concat([mun_sup,tot_sup_mun],ignore_index=True).round(1))
    return out
def clean_isq_name(val):
    # ISQ appends footnote markers to some names (ex.: "Papineau2") → strip trailing digits
    if val is None: return None
    return re.sub(r"\d+$","",str(val).strip()).strip() or None
def build_population_data(match_df):
    our_mrcs=set(match_df["CDNAME"].unique())
    hist_path=Path(POP_HIST_PATH); proj_path=Path(POP_PROJ_PATH)
    if not hist_path.exists() or not proj_path.exists():
        print(f"  ⚠  {POP_HIST_PATH} ou {POP_PROJ_PATH} introuvable – données pop ignorées"); return
    try: import openpyxl
    except ImportError: print("  ⚠  openpyxl manquant (pip install openpyxl)"); return
    print("\n══ Population historique (pop-hist-mrc.xlsx) ══")
    wb=openpyxl.load_workbook(hist_path,read_only=True); ws=wb.active
    rows=list(ws.iter_rows(values_only=True))
    year_row=[str(v).replace('r','').replace('p','') for v in rows[2][3:]]
    year_ints=[int(y) for y in year_row if y.isdigit()]
    hist_out=[]
    for r in rows[4:]:
        mrc_name=clean_isq_name(r[2])
        if not mrc_name or mrc_name not in our_mrcs: continue
        for i,yr in enumerate(year_ints):
            if yr<2012: continue
            val=r[3+i]
            try: pop=int(val)
            except (TypeError,ValueError): continue
            hist_out.append({"CDNAME":mrc_name,"Annee":yr,"Population":pop})
    save_json(pd.DataFrame(hist_out),"population_mrc.json")
    print(f"  ✓ {len(hist_out)} lignes historiques")
    print("\n══ Projections de population (pop-proj-mrc.xlsx) ══")
    wb2=openpyxl.load_workbook(proj_path,read_only=True); ws2=wb2.active
    rows2=list(ws2.iter_rows(values_only=True))
    proj_years=[v for v in rows2[5][3:] if isinstance(v,int)]
    SCENARIO_MAP={"Référence (A2025)":"reference","Fort (E2025)":"fort","Faible (D2025)":"faible"}
    proj_out=[]
    for r in rows2[7:]:
        if not r[0] or not r[2]: continue
        scenario_raw=str(r[0]).strip(); mrc_name=clean_isq_name(r[2])
        scenario=SCENARIO_MAP.get(scenario_raw)
        if not scenario or mrc_name not in our_mrcs: continue
        for i,yr in enumerate(proj_years):
            val=r[3+i]
            try: pop=int(val)
            except (TypeError,ValueError): continue
            proj_out.append({"CDNAME":mrc_name,"Annee":yr,"Scenario":scenario,"Population":pop})
    save_json(pd.DataFrame(proj_out),"projections_pop_mrc.json")
    print(f"  ✓ {len(proj_out)} lignes projetées")
    men_path=Path(MENAGES_PROJ_PATH)
    if not men_path.exists(): print(f"  ⚠  {MENAGES_PROJ_PATH} introuvable – ménages ignorés"); return
    print("\n══ Projections de ménages (menages-proj-mrc.xlsx) ══")
    wb3=openpyxl.load_workbook(men_path,read_only=True); ws3=wb3.active
    rows3=list(ws3.iter_rows(values_only=True))
    men_years=[v for v in rows3[5][3:] if isinstance(v,int)]
    men_out=[]
    for r in rows3[7:]:
        if not r[0] or not r[2]: continue
        scenario=SCENARIO_MAP.get(str(r[0]).strip()); mrc_name=clean_isq_name(r[2])
        if not scenario or mrc_name not in our_mrcs: continue
        for i,yr in enumerate(men_years):
            val=r[3+i]
            try: men=int(val)
            except (TypeError,ValueError): continue
            men_out.append({"CDNAME":mrc_name,"Annee":yr,"Scenario":scenario,"Menages":men})
    save_json(pd.DataFrame(men_out),"menages_proj_mrc.json")
    print(f"  ✓ {len(men_out)} lignes ménages")
def main():
    MATCH=load_match(); our_mrcs=set(MATCH["CDNAME"].unique())
    region_by_mrc=MATCH.drop_duplicates("CDNAME").set_index("CDNAME")["Region"].to_dict()
    pf_lookup,region_names=load_pf_mun(our_mrcs)
    print(f"\nSHP zips : {dict(sorted(SHP_ZIPS.items())) or 'aucun'}")
    print(f"PU  zips : {dict(sorted(PU_ZIPS.items())) or 'aucun'}")
    # Le détail municipal est éclaté par région : on repart d'un dossier propre pour
    # qu'aucun fragment d'une exécution précédente (région retirée, MRC renommée) ne survive.
    if MUN_DIR.exists(): shutil.rmtree(MUN_DIR)
    # Une année à la fois : agrégée puis libérée. Tous les indicateurs exportés sont
    # ventilés par année, l'empilement des agrégats donne donc les mêmes fichiers
    # qu'un traitement en bloc, pour une fraction de la mémoire.
    acc={}; qa_by_year={}
    for annee,frames in year_batches(MATCH,pf_lookup):
        Role_brut=pd.concat(frames,ignore_index=True); frames.clear()
        qa_by_year[annee]=(Role_brut["CSDNAME"].nunique(),len(Role_brut))
        print(f"\n✓ Brut {annee} : {len(Role_brut):,} UE")
        for mode in MODES:
            for stem,(scope,frame) in indicator_frames(Role_brut,mode=mode,suffix=mode).items():
                acc.setdefault(stem,(scope,[]))[1].append(frame)
        del Role_brut
    if not acc: print("Aucune donnée."); return
    annees=sorted(qa_by_year)
    print(f"\n✓ {len(annees)} années ({annees})")
    print("\nÉcriture des indicateurs par mode de catégorisation...")
    for stem,(scope,parts) in acc.items():
        df=pd.concat(parts,ignore_index=True)
        if scope=="mun": save_json_mun(df,stem,region_by_mrc)
        else: save_json(df,f"{stem}.json")
    acc.clear()
    # QA couverture : nb de municipalités et d'UE par année (années absentes incluses)
    n_attendu=len(MATCH)
    dropped_by_year={d["annee"]:d["raison"] for d in QA_DROPPED_YEARS}
    qa_rows=[]
    for a in range(ANNEE_MIN,max(annees)+1):
        if a in qa_by_year:
            n_mun,n_ue=qa_by_year[a]
            note=None if n_mun>=n_attendu else f"{n_attendu-n_mun} municipalité(s) manquante(s)"
        else:
            n_mun=0; n_ue=0
            note=dropped_by_year.get(a,"année absente des sources")
        qa_rows.append({"Annee":a,"n_mun":n_mun,"n_mun_attendu":n_attendu,"n_ue":n_ue,
                        "complet":n_mun>=n_attendu,"note":note})
        if note: print(f"  ⚠  QA {a} : {note} ({n_mun}/{n_attendu} mun)")
    save_json(pd.DataFrame(qa_rows),"qa_couverture.json")
    mrc_list=MATCH.groupby("CDNAME")["Municipalité"].apply(list).reset_index(); mrc_list.columns=["CDNAME","municipalites"]
    mrc_list["Region"]=mrc_list["CDNAME"].map(region_by_mrc)
    save_json(mrc_list,"mrc_list.json")
    # Le sélecteur de région du tableau de bord est construit à partir de ce fichier :
    # il suit le contenu de MATCH.csv au lieu d'une liste figée dans la page.
    regions=sorted({int(r) for r in region_by_mrc.values() if pd.notna(r)})
    save_json(pd.DataFrame([{"Region":r,"nom":region_names.get(r,str(r))} for r in regions]),"regions.json")
    build_indicateurs_pu(pf_lookup,region_by_mrc)
    build_population_data(MATCH)
    print("\n🎉 Pipeline terminé.")
if __name__=="__main__":
    main()
