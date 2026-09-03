"""Audit ponctuel de la distribution des codes RL0105A (CUBF) du rôle.

Répond à la question : quels codes CUBF sont réellement présents dans les
unités d'évaluation de nos municipalités (notamment le code 1010 retenu par la
fiche méthodologique OGAT) ?

Usage : python tools/audit_cubf.py [annee]
"""
import os, re, sys, time
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import requests
import pipeline as P

MAMH_BASE = [1000, 1010, 1211, 1702]
MAMH_OPT = [1543, 1549]


def main():
    annees = [int(a) for a in sys.argv[1:]] or [2026]
    match = P.load_match()
    pf = P.load_pf_mun(set(match["CDNAME"].dropna().unique()))
    mun_to_mrc = match.set_index("_mun_key")[["CDNAME", "Region"]].to_dict("index")
    muns, our_codes = set(mun_to_mrc), set(pf)

    for annee in annees:
        index = P.fetch_index(annee)
        if not index:
            print(f"Index {annee} indisponible"); continue
        counter, n_mun, n_ue = Counter(), 0, 0
        muns_1010 = Counter()
        for code, nom_mun, url_xml in index:
            if nom_mun not in muns: continue
            if re.match(r"^\d{5}$", code) and code not in our_codes: continue
            if re.match(r"^\d{5}$", code):
                base = url_xml.rsplit("/", 1)[0]
                expected = f"RL{code}_{annee}.xml"
                if url_xml.rsplit("/", 1)[-1] != expected:
                    url_xml = f"{base}/{expected}"
            meta = mun_to_mrc[nom_mun]
            try:
                r = requests.get(url_xml, timeout=60); r.raise_for_status()
                rows = P.parse_xml(r.content, annee, nom_mun, meta["CDNAME"], meta["Region"])
            except Exception as e:
                print(f"  ERR {nom_mun}: {e}"); continue
            n_mun += 1; n_ue += len(rows)
            for row in rows:
                v = (row["rl0105a"] or "").strip()
                counter[v] += 1
                if v == "1010": muns_1010[nom_mun] += 1
            time.sleep(0.1)

        print(f"\n══ Distribution RL0105A — {annee} ══")
        print(f"{n_mun} municipalités, {n_ue:,} unités d'évaluation")
        print("\n-- Codes de la fiche OGAT --")
        for c in MAMH_BASE + MAMH_OPT:
            print(f"  {c}: {counter.get(str(c), 0):,}")
        n5 = sum(v for k, v in counter.items() if k.isdigit() and 5000 <= int(k) <= 5999)
        n81 = sum(v for k, v in counter.items() if k.isdigit() and 8100 <= int(k) <= 8199)
        print(f"  5000-5999: {n5:,}")
        print(f"  8100-8199: {n81:,}")
        print("\n-- Groupe 1000-1099 (voisinage du code 1010) --")
        for k in sorted(k for k in counter if k.isdigit() and 1000 <= int(k) <= 1099):
            print(f"  {k}: {counter[k]:,}")
        if muns_1010:
            print("\n-- Municipalités avec des 1010 --")
            for m, n in muns_1010.most_common():
                print(f"  {m}: {n:,}")
        print("\n-- Tous les codes du groupe 1 (résidentiel) --")
        for k in sorted(k for k in counter if k.isdigit() and 1000 <= int(k) <= 1999):
            print(f"  {k}: {counter[k]:,}")


if __name__ == "__main__":
    main()
