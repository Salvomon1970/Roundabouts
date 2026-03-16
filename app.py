import streamlit as st
import pandas as pd
import requests
import time
import math
import random
import io
from math import radians, sin, cos, sqrt, atan2
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(page_title="Analisi Rotatorie SLiM", page_icon="🗺️")

sessione = requests.Session()
sessione.headers.update({'User-Agent': 'Script_Analisi_Infrastrutture_Universita/4.0'})

def invia_query_osm(query):
    endpoints = [
        "https://lz4.overpass-api.de/api/interpreter",
        "https://z.overpass-api.de/api/interpreter",
        "https://overpass-api.de/api/interpreter"
    ]
    attesa = 2
    for tentativo in range(15):
        url = random.choice(endpoints)
        try:
            risposta = sessione.get(url, params={'data': query}, timeout=25)
            if risposta.status_code == 200:
                return risposta.json()
            elif risposta.status_code == 429:
                time.sleep(attesa)
                attesa = min(attesa + 3, 20)
            else:
                time.sleep(2)
        except Exception:
            time.sleep(attesa)
    return None

def check_rotatoria(lat, lon):
    query = f"""
    [out:json];
    (way["junction"="roundabout"](around:75, {lat}, {lon}););
    out ids;
    """
    dati = invia_query_osm(query)
    if dati and 'elements' in dati and len(dati['elements']) > 0:
        return "sì"
    return "no"

def calcola_distanza(lat1, lon1, lat2, lon2):
    R = 6371000
    a = sin(radians(lat2 - lat1) / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(radians(lon2 - lon1) / 2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))

def metodo_clustering(lat, lon, raggio):
    query = f"""
    [out:json];
    (
      way["junction"~"roundabout|circular|rotary"](around:{raggio}, {lat}, {lon});
      relation["junction"~"roundabout|circular|rotary"](around:{raggio}, {lat}, {lon});
      way["highway"~"mini_roundabout|turning_circle|turning_loop"](around:{raggio}, {lat}, {lon});
    );
    out geom;
    """
    dati = invia_query_osm(query)
    if not dati: return None

    poligoni = []
    for e in dati.get('elements', []):
        punti = []
        if e['type'] == 'way':
            punti = [(pt['lat'], pt['lon']) for pt in e.get('geometry', [])]
        elif e['type'] == 'relation':
            for m in e.get('members', []):
                if m['type'] == 'way' and 'geometry' in m:
                    punti.extend([(pt['lat'], pt['lon']) for pt in m['geometry']])
        if len(punti) >= 2:
            poligoni.append(punti)

    if not poligoni: return None

    cluster = []
    for p in poligoni:
        connessi, rimasti = [], []
        for c in cluster:
            vicini = False
            for pt1 in p:
                for pt2 in c:
                    if calcola_distanza(pt1[0], pt1[1], pt2[0], pt2[1]) < 15:
                        vicini = True
                        break
                if vicini: break
            if vicini: connessi.append(c)
            else: rimasti.append(c)
        nuovo = list(p)
        for c in connessi: nuovo.extend(c)
        rimasti.append(nuovo)
        cluster = rimasti

    miglior_diametro = None
    distanza_minima = float('inf')
    for c in cluster:
        lat_med = sum(pt[0] for pt in c) / len(c)
        lon_med = sum(pt[1] for pt in c) / len(c)
        dist_centro = calcola_distanza(lat, lon, lat_med, lon_med)

        diametro = 0
        for i in range(len(c)):
            for j in range(i+1, len(c)):
                d = calcola_distanza(c[i][0], c[i][1], c[j][0], c[j][1])
                if d > diametro: diametro = d

        if dist_centro < distanza_minima:
            distanza_minima = dist_centro
            miglior_diametro = diametro

    return round(miglior_diametro, 2) if miglior_diametro else None

def metodo_topologico(lat, lon, raggio):
    query = f"""
    [out:json];
    (
      way["junction"~"roundabout|circular|rotary"](around:{raggio}, {lat}, {lon});
      relation["junction"~"roundabout|circular|rotary"](around:{raggio}, {lat}, {lon});
    );
    out body;
    >;
    out skel qt;
    """
    dati = invia_query_osm(query)
    if not dati: return None

    nodi = {n['id']: (n['lat'], n['lon']) for n in dati.get('elements', []) if n['type'] == 'node'}
    vie = [v for v in dati.get('elements', []) if v['type'] == 'way']

    entita = []
    for via in vie:
        coords = [nodi[nid] for nid in via.get('nodes', []) if nid in nodi]
        if len(coords) >= 2: entita.append(coords)

    if not entita: return None

    miglior_diametro = None
    distanza_minima = float('inf')
    for coords in entita:
        lat_med = sum(pt[0] for pt in coords) / len(coords)
        lon_med = sum(pt[1] for pt in coords) / len(coords)
        dist = calcola_distanza(lat, lon, lat_med, lon_med)

        diametro = 0
        for i in range(len(coords)):
            for j in range(i+1, len(coords)):
                d = calcola_distanza(coords[i][0], coords[i][1], coords[j][0], coords[j][1])
                if d > diametro: diametro = d

        if dist < distanza_minima:
            distanza_minima = dist
            miglior_diametro = diametro

    return round(miglior_diametro, 2) if miglior_diametro else None

def metodo_nodi(lat, lon, raggio):
    query = f"""
    [out:json];
    node["highway"~"mini_roundabout|turning_circle|turning_loop"](around:{raggio}, {lat}, {lon});
    out body;
    """
    dati = invia_query_osm(query)
    if dati and len(dati.get('elements', [])) > 0:
        return 7.0
    return None

def calcola_diametro_integrato(lat, lon):
    diametro = metodo_clustering(lat, lon, 45)
    if diametro and 5 <= diametro <= 90: return diametro
    
    diametro = metodo_topologico(lat, lon, 80)
    if diametro and 5 <= diametro <= 120: return diametro
    
    diametro = metodo_clustering(lat, lon, 120)
    if diametro and 5 <= diametro <= 150: return diametro
    
    diametro = metodo_nodi(lat, lon, 80)
    if diametro: return diametro
    
    return None

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def conta_rami_assoluto(lat, lon):
    query = f"""
    [out:json];
    (
      way["highway"](around:70, {lat}, {lon});
      node["highway"="mini_roundabout"](around:70, {lat}, {lon});
    );
    (._;>;);
    out body qt;
    """
    dati = invia_query_osm(query)
    if not dati: return None

    nodi = {n['id']: (n['lat'], n['lon']) for n in dati.get('elements', []) if n['type'] == 'node'}
    vie = {v['id']: v for v in dati.get('elements', []) if v['type'] == 'way'}
    
    rotatorie_ways = []
    nodo_mini = None

    for vid, v in vie.items():
        tags = v.get('tags', {})
        if tags.get('junction') in ['roundabout', 'circular', 'rotary'] or tags.get('highway') in ['turning_loop', 'turning_circle']:
            rotatorie_ways.append(v)

    for e in dati.get('elements', []):
        if e['type'] == 'node' and e.get('tags', {}).get('highway') == 'mini_roundabout':
            nodo_mini = (e['lat'], e['lon'])
            break

    target_r_nodes = set()
    
    if rotatorie_ways:
        cluster_rot = []
        for w in rotatorie_ways:
            w_nodes = set(w.get('nodes', []))
            added = False
            for c in cluster_rot:
                if c.intersection(w_nodes):
                    c.update(w_nodes)
                    added = True
                    break
            if not added:
                cluster_rot.append(w_nodes)
                
        min_dist_to_cluster = float('inf')
        best_cluster = None
        for c in cluster_rot:
            c_nodes = [nodi[nid] for nid in c if nid in nodi]
            if not c_nodes: continue
            c_lat = sum(pt[0] for pt in c_nodes) / len(c_nodes)
            c_lon = sum(pt[1] for pt in c_nodes) / len(c_nodes)
            dist = haversine(lat, lon, c_lat, c_lon)
            if dist < min_dist_to_cluster:
                min_dist_to_cluster = dist
                best_cluster = c
                
        if best_cluster:
            for nid in best_cluster:
                if nid in nodi:
                    target_r_nodes.add(nodi[nid])
                    
    if not target_r_nodes and nodo_mini:
        target_r_nodes.add(nodo_mini)

    if not target_r_nodes: return None

    outward_points = []
    esclusi = ['pedestrian', 'footway', 'path', 'cycleway', 'steps', 'service', 'track', 'construction', 'proposed', 'corridor', 'elevator']
    
    for vid, v in vie.items():
        tags = v.get('tags', {})
        if tags.get('junction') in ['roundabout', 'circular', 'rotary'] or tags.get('highway') in ['turning_loop', 'turning_circle']:
            continue
        if tags.get('highway') in esclusi or 'highway' not in tags:
            continue
            
        way_nodes = [nodi[nid] for nid in v.get('nodes', []) if nid in nodi]
        intersect_idx = [i for i, pt in enumerate(way_nodes) if pt in target_r_nodes]
        
        if not intersect_idx: continue
            
        first_idx = intersect_idx[0]
        pts_bwd = []
        for j in range(first_idx - 1, -1, -1):
            if way_nodes[j] in target_r_nodes: break
            pts_bwd.append(way_nodes[j])
            
        if pts_bwd:
            best_pt = None
            min_diff = float('inf')
            max_dist = -1
            furthest_pt = None
            for pt in pts_bwd:
                dist_R = min(haversine(pt[0], pt[1], r[0], r[1]) for r in target_r_nodes)
                if dist_R > max_dist:
                    max_dist = dist_R
                    furthest_pt = pt
                diff = abs(dist_R - 25) 
                if diff < min_diff:
                    min_diff = diff
                    best_pt = pt
            if max_dist >= 8:
                outward_points.append(best_pt if max_dist >= 20 else furthest_pt)

        last_idx = intersect_idx[-1]
        pts_fwd = []
        for j in range(last_idx + 1, len(way_nodes)):
            if way_nodes[j] in target_r_nodes: break
            pts_fwd.append(way_nodes[j])
            
        if pts_fwd:
            best_pt = None
            min_diff = float('inf')
            max_dist = -1
            furthest_pt = None
            for pt in pts_fwd:
                dist_R = min(haversine(pt[0], pt[1], r[0], r[1]) for r in target_r_nodes)
                if dist_R > max_dist:
                    max_dist = dist_R
                    furthest_pt = pt
                diff = abs(dist_R - 25)
                if diff < min_diff:
                    min_diff = diff
                    best_pt = pt
            if max_dist >= 8:
                outward_points.append(best_pt if max_dist >= 20 else furthest_pt)

    if not outward_points: return None
        
    clusters = []
    for pt in outward_points:
        found = False
        for c in clusters:
            for cpt in c:
                if haversine(pt[0], pt[1], cpt[0], cpt[1]) < 25:
                    c.append(pt)
                    found = True
                    break
            if found: break
        if not found:
            clusters.append([pt])
            
    numero_rami = len(clusters)
    
    if numero_rami < 3: return 3
    if numero_rami > 6: return 6
    return numero_rami

def elabora_singolo_nodo(idx, lat, lon):
    esito = check_rotatoria(lat, lon)
    diametro = None
    rami = None
    if esito in ['sì', 'si', 'yes']:
        diametro = calcola_diametro_integrato(lat, lon)
        rami = conta_rami_assoluto(lat, lon)
    return idx, esito, diametro, rami

st.title("🗺️ Analisi Rotatorie SLiM")
st.markdown("Carica un file Excel contenente le coordinate per analizzare le rotatorie, calcolarne il diametro e i rami.")

if 'analisi_in_corso' not in st.session_state:
    st.session_state['analisi_in_corso'] = False

file_caricato = st.file_uploader("Scegli un file Excel (.xlsx)", type=["xlsx"])

if file_caricato is not None:
    if 'df_elaborato' not in st.session_state or not st.session_state['analisi_in_corso']:
        df_iniziale = pd.read_excel(file_caricato)
        for col in ['Rotatoria', 'Diametro_Esterno_m', 'Numero di rami']:
            if col not in df_iniziale.columns:
                df_iniziale[col] = None
        if 'df_elaborato' not in st.session_state:
            st.session_state['df_elaborato'] = df_iniziale

    st.write("Anteprima dei dati:")
    st.dataframe(st.session_state['df_elaborato'].head(3))
    
    if st.button("🚀 Avvia elaborazione", type="primary") and not st.session_state['analisi_in_corso']:
        st.session_state['analisi_in_corso'] = True
        st.rerun()
        
    if st.session_state['analisi_in_corso']:
        df = st.session_state['df_elaborato']
        indici_da_elaborare = df[df['Rotatoria'].isna()].index.tolist()
        
        if not indici_da_elaborare:
            st.info("Tutti i nodi nel file risultano già elaborati.")
            st.session_state['analisi_in_corso'] = False
        else:
            totale_righe = len(df)
            elaborati_finora = totale_righe - len(indici_da_elaborare)
            
            st.markdown("### Elaborazione lotti in corso...")
            progress_bar = st.progress(elaborati_finora / totale_righe)
            st.text(f"Completati {elaborati_finora} su {totale_righe} nodi...")
            
            lotto = indici_da_elaborare[:10]
            
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {
                    executor.submit(elabora_singolo_nodo, idx, df.at[idx, 'Latitudine'], df.at[idx, 'Longitudine']): idx 
                    for idx in lotto
                }
                
                for future in as_completed(futures):
                    idx = futures[future]
                    try:
                        idx_res, esito, diametro, rami = future.result()
                        df.at[idx, 'Rotatoria'] = esito
                        if diametro is not None:
                            df.at[idx, 'Diametro_Esterno_m'] = diametro
                        if rami is not None:
                            df.at[idx, 'Numero di rami'] = rami
                    except Exception:
                        df.at[idx, 'Rotatoria'] = 'errore'

            st.session_state['df_elaborato'] = df
            time.sleep(1)
            st.rerun()

if 'df_elaborato' in st.session_state and not st.session_state.get('analisi_in_corso', False):
    df_finale = st.session_state['df_elaborato']
    if df_finale['Rotatoria'].notna().any():
        st.markdown("---")
        st.markdown("### Risultato pronto")
        
        output = io.BytesIO()
        df_finale.to_excel(output, index=False)
        output.seek(0)
        
        st.download_button(
            label="⬇️ Scarica il File Definitivo (.xlsx)",
            data=output,
            file_name="Analisi_Infrastrutture_Completata.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
