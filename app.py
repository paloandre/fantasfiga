# app.py - COMPLETO CON MODIFICHE
from flask import Flask, render_template, request, jsonify, send_file
import pandas as pd
import os
import random
import math
import numpy as np
from scipy.stats import norm
from werkzeug.utils import secure_filename
from datetime import datetime
from itertools import combinations
from collections import defaultdict

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

PUNTI_VITTORIA = 3
PUNTI_PAREGGIO = 1
PUNTI_SCONFITTA = 0

# Memorizza l'ultimo calendario reale per poterlo modificare
ultimo_calendario_reale = None
ultime_squadre = []
nome_lega = ""

os.makedirs(UPLOAD_FOLDER, exist_ok=True)


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def estrai_nome_lega(df_grezzo):
    """Estrae il nome della lega dalla riga 1 (indice 0), celle unite A:K"""
    try:
        # La riga 1 è l'indice 0 in pandas
        valore = str(df_grezzo.iloc[0, 0])
        # Rimuove "Calendario" se presente
        valore = valore.replace("Calendario", "").strip()
        return valore
    except:
        return "Lega"


def parse_calendario(df_grezzo):
    """Parsing SAS → Python (calendario reale)"""
    df = df_grezzo.copy()
    
    mask_b_missing = df[1].isna() | (df[1] == '')
    df.loc[mask_b_missing, 0] = df.loc[mask_b_missing, 2]
    df.loc[mask_b_missing, 6] = df.loc[mask_b_missing, 8]
    
    df['cn'] = (df[3].isna() | (df[3] == '')).astype(int)
    df = df.iloc[3:].reset_index(drop=True)
    
    _cal1 = pd.DataFrame()
    _cal1['squadra1'] = df[0].astype(str).str.strip()
    _cal1['punteggio1'] = pd.to_numeric(df[1], errors='coerce')
    _cal1['punteggio2'] = df[2].astype(str).str.strip()
    _cal1['squadra2'] = df[3].astype(str).str.strip()
    _cal1['risultato'] = df[4].astype(str).str.strip()
    _cal1['cn'] = df['cn']
    
    _cal2 = pd.DataFrame()
    _cal2['squadra1'] = df[6].astype(str).str.strip()
    _cal2['punteggio1'] = pd.to_numeric(df[7], errors='coerce')
    _cal2['punteggio2'] = df[8].astype(str).str.strip()
    _cal2['squadra2'] = df[9].astype(str).str.strip()
    _cal2['risultato'] = df[10].astype(str).str.strip()
    _cal2['cn'] = df['cn']
    
    _cal = pd.concat([_cal1, _cal2], ignore_index=True)
    _cal = _cal[_cal['squadra1'].notna() & (_cal['squadra1'] != '') & (_cal['squadra1'] != 'nan')]
    _cal['counter'] = _cal['cn'].cumsum()
    
    mask_missing_s2 = _cal['squadra2'].isna() | (_cal['squadra2'] == '') | (_cal['squadra2'] == 'nan')
    for idx in _cal[mask_missing_s2].index:
        s1 = str(_cal.loc[idx, 'squadra1'])
        if len(s1) >= 2:
            second_char = s1[1]
            if second_char in ('0','1','2','3','4','5','6','7','8','9'):
                _cal.loc[idx, 'squadra1'] = s1[:2]
            else:
                _cal.loc[idx, 'squadra1'] = s1[0]
        elif len(s1) == 1:
            _cal.loc[idx, 'squadra1'] = s1[0]
    
    _map_cal = _cal[mask_missing_s2].copy()
    _map_cal = _map_cal[['squadra1', 'counter']].rename(columns={'squadra1': 'giornata'})
    
    _calendario = _cal.merge(_map_cal, on='counter', how='left')
    _calendario['risultato_clean'] = _calendario['risultato'].astype(str).str.replace(' ', '')
    mask_valid = (
        (_calendario['giornata'] != _calendario['squadra1']) & 
        (_calendario['risultato_clean'] != '-')
    )
    _calendario = _calendario[mask_valid].copy()
    
    result = pd.DataFrame()
    result['squadra1'] = _calendario['squadra1'].str.upper().str.strip()
    result['squadra2'] = _calendario['squadra2'].str.upper().str.strip()
    result['punteggio1'] = pd.to_numeric(_calendario['punteggio1'], errors='coerce')
    result['punteggio2'] = pd.to_numeric(_calendario['punteggio2'], errors='coerce')
    
    def parse_risultato(ris):
        if pd.isna(ris) or ris == '':
            return pd.Series([None, None])
        parts = str(ris).split('-')
        if len(parts) == 2:
            return pd.Series([
                pd.to_numeric(parts[0].strip(), errors='coerce'),
                pd.to_numeric(parts[1].strip(), errors='coerce')
            ])
        return pd.Series([None, None])
    
    result[['gol1', 'gol2']] = _calendario['risultato'].apply(parse_risultato)
    result['num_giornata'] = pd.to_numeric(_calendario['giornata'], errors='coerce')
    
    result = result[
        result['squadra1'].notna() & 
        result['squadra2'].notna() & 
        (result['squadra1'] != '') & 
        (result['squadra2'] != '') &
        (result['squadra1'] != 'NAN') &
        (result['squadra2'] != 'NAN')
    ].reset_index(drop=True)
    
    return result


def crea_fantasy_long(calendario_reale):
    """Crea dataset FANTASY in formato LONG."""
    fantasy_rows = []
    
    for giornata, group in calendario_reale.groupby('num_giornata'):
        squadre_stats = {}
        
        for _, row in group.iterrows():
            squadre_stats[row['squadra1']] = row['punteggio1']
            squadre_stats[row['squadra2']] = row['punteggio2']
        
        lista_squadre = sorted(squadre_stats.keys())
        
        for squadra_a, squadra_b in combinations(lista_squadre, 2):
            p_a = squadre_stats[squadra_a] if pd.notna(squadre_stats[squadra_a]) else 0
            p_b = squadre_stats[squadra_b] if pd.notna(squadre_stats[squadra_b]) else 0
            
            pt_a, pt_b = calcola_punti_partita_fantasy(p_a, p_b)
            
            fantasy_rows.append({
                'squadra': squadra_a,
                'avversario': squadra_b,
                'giornata': int(giornata),
                'punteggio_proprio': p_a,
                'punteggio_avversario': p_b,
                'punteggio': f"{p_a}-{p_b}",
                'punti_ottenuti': pt_a
            })
            
            fantasy_rows.append({
                'squadra': squadra_b,
                'avversario': squadra_a,
                'giornata': int(giornata),
                'punteggio_proprio': p_b,
                'punteggio_avversario': p_a,
                'punteggio': f"{p_b}-{p_a}",
                'punti_ottenuti': pt_b
            })
    
    df = pd.DataFrame(fantasy_rows)
    df = df.sort_values(by=['squadra', 'giornata', 'avversario']).reset_index(drop=True)
    
    return df


def calcola_punti_partita_fantasy(p1, p2):
    """Regola fantasy con soglia 66."""
    SOGLIA = 66
    
    if p1 < SOGLIA and p2 < SOGLIA:
        return PUNTI_PAREGGIO, PUNTI_PAREGGIO
    
    if p1 > p2:
        return PUNTI_VITTORIA, PUNTI_SCONFITTA
    elif p2 > p1:
        return PUNTI_SCONFITTA, PUNTI_VITTORIA
    else:
        return PUNTI_PAREGGIO, PUNTI_PAREGGIO


def calcola_classifica_reale(df_reale):
    """Calcola classifica REALE basata sui GOL."""
    stats = defaultdict(lambda: {
        'punti': 0,
        'giocate': 0,
        'vinte': 0,
        'pareggiate': 0,
        'perse': 0,
        'gol_fatti': 0,
        'gol_subiti': 0,
        'punti_fatti': 0,
        'punti_subiti': 0
    })
    
    for _, row in df_reale.iterrows():
        s1, s2 = row['squadra1'], row['squadra2']
        g1 = int(row['gol1']) if pd.notna(row['gol1']) else 0
        g2 = int(row['gol2']) if pd.notna(row['gol2']) else 0

        p1 = float(row['punteggio1']) if pd.notna(row['punteggio1']) else 0
        p2 = float(row['punteggio2']) if pd.notna(row['punteggio2']) else 0

        stats[s1]['punti_fatti'] += p1
        stats[s1]['punti_subiti'] += p2

        stats[s2]['punti_fatti'] += p2
        stats[s2]['punti_subiti'] += p1
        
        stats[s1]['giocate'] += 1
        stats[s2]['giocate'] += 1
        
        stats[s1]['gol_fatti'] += g1
        stats[s1]['gol_subiti'] += g2
        stats[s2]['gol_fatti'] += g2
        stats[s2]['gol_subiti'] += g1
        
        if g1 > g2:
            stats[s1]['punti'] += PUNTI_VITTORIA
            stats[s1]['vinte'] += 1
            stats[s2]['perse'] += 1
        elif g2 > g1:
            stats[s2]['punti'] += PUNTI_VITTORIA
            stats[s2]['vinte'] += 1
            stats[s1]['perse'] += 1
        else:
            stats[s1]['punti'] += PUNTI_PAREGGIO
            stats[s2]['punti'] += PUNTI_PAREGGIO
            stats[s1]['pareggiate'] += 1
            stats[s2]['pareggiate'] += 1
    
    classifica = []
    for squadra, s in stats.items():
        classifica.append({
            'squadra': squadra,
            'punti_reali': s['punti'],
            'giocate': s['giocate'],
            'vinte': s['vinte'],
            'pareggiate': s['pareggiate'],
            'perse': s['perse'],
            'gol_fatti': s['gol_fatti'],
            'gol_subiti': s['gol_subiti'],
            'differenza_rete': s['gol_fatti'] - s['gol_subiti'],
            'punti_fatti': round(s['punti_fatti'],2),
            'punti_subiti': round(s['punti_subiti'],2)
        })
    
    df = pd.DataFrame(classifica)
    df = df.sort_values(
        by=['punti_reali', 'differenza_rete', 'gol_fatti'], 
        ascending=[False, False, False]
    ).reset_index(drop=True)
    df['posizione_reale'] = range(1, len(df) + 1)
    
    return df


def calcola_classifica_fantasy_da_long(df_fantasy_long):
    """Calcola classifica fantasy dal dataset long."""
    num_squadre = df_fantasy_long['squadra'].nunique()
    partite_per_giornata = num_squadre - 1
    
    stats = df_fantasy_long.groupby('squadra').agg({
        'punti_ottenuti': 'sum',
        'squadra': 'count'
    }).rename(columns={'squadra': 'giocate', 'punti_ottenuti': 'punti_raw'})
    
    stats['punti_fantasy'] = stats['punti_raw'] / partite_per_giornata
    
    vps = df_fantasy_long.groupby('squadra')['punti_ottenuti'].value_counts().unstack(fill_value=0)
    
    for col in [PUNTI_SCONFITTA, PUNTI_PAREGGIO, PUNTI_VITTORIA]:
        if col not in vps.columns:
            vps[col] = 0
    
    stats['vinte'] = vps[PUNTI_VITTORIA]
    stats['pareggiate'] = vps[PUNTI_PAREGGIO]
    stats['perse'] = vps[PUNTI_SCONFITTA]
    
    classifica = []
    for squadra in stats.index:
        classifica.append({
            'squadra': squadra,
            'punti_fantasy': round(stats.loc[squadra, 'punti_fantasy'], 2),
            'punti_fantasy_raw': int(stats.loc[squadra, 'punti_raw']),
            'giocate': int(stats.loc[squadra, 'giocate']),
            'vinte': int(stats.loc[squadra, 'vinte']),
            'pareggiate': int(stats.loc[squadra, 'pareggiate']),
            'perse': int(stats.loc[squadra, 'perse'])
        })
    
    df = pd.DataFrame(classifica)
    df = df.sort_values(
        by=['punti_fantasy', 'punti_fantasy_raw'], 
        ascending=[False, False]
    ).reset_index(drop=True)
    df['posizione_fantasy'] = range(1, len(df) + 1)
    
    return df


def calcola_forza_avversari(df_reale):
    """Calcola forza degli avversari - VERSIONE MODIFICATA"""
    partite_per_squadra = defaultdict(list)
    
    # Prima passata: raccogli tutte le partite per squadra
    for _, row in df_reale.iterrows():
        s1, s2 = row['squadra1'], row['squadra2']
        g1 = float(row['gol1']) if pd.notna(row['gol1']) else 0
        g2 = float(row['gol2']) if pd.notna(row['gol2']) else 0
        
        partite_per_squadra[s1].append({
            'avversario': s2,
            'gol_subiti': g2,
            'gol_fatti': g1
        })
        
        partite_per_squadra[s2].append({
            'avversario': s1,
            'gol_subiti': g1,
            'gol_fatti': g2
        })
    
    risultati = []
    
    for squadra, partite in partite_per_squadra.items():
        # === MEDIA GOL AVVERSARI VS ME (INVARIATO) ===
        gol_avversari_vs_me = [p['gol_subiti'] for p in partite]
        media_vs_me = round(sum(gol_avversari_vs_me) / len(gol_avversari_vs_me), 2) if gol_avversari_vs_me else 0
        
        # === NUOVO: MEDIA GOL ALTRI NELLE PARTITE SENZA DI ME ===
        # Trova tutte le partite in cui questa squadra NON è coinvolta
        # e calcola la media di TUTTI i gol in quelle partite
        
        # Crea set di avversari incontrati (partite "contaminate" dalla squadra)
        avversari_incontrati = set([p['avversario'] for p in partite])
        
        # Raccogli tutti i gol dalle partite dove la squadra NON gioca
        gol_altre_partite = []
        
        for _, row in df_reale.iterrows():
            s1, s2 = row['squadra1'], row['squadra2']
            
            # Se la squadra non è in questa partita, prendi entrambi i gol
            if s1 != squadra and s2 != squadra:
                g1 = float(row['gol1']) if pd.notna(row['gol1']) else 0
                g2 = float(row['gol2']) if pd.notna(row['gol2']) else 0
                gol_altre_partite.extend([g1, g2])
        
        # Calcola media su TUTTI i gol delle partite senza la squadra
        media_altre_partite = round(sum(gol_altre_partite) / len(gol_altre_partite), 2) if gol_altre_partite else 0
        
        risultati.append({
            'squadra': squadra,
            'partite_giocate': len(partite),
            'media_gol_avversari_vs_me': media_vs_me,
            'media_gol_avversari_altre': media_altre_partite,  # Ora è la media gol della lega nelle partite senza di lei
            'difficolta_calendario': round(media_vs_me - media_altre_partite, 2)
        })
    
    df = pd.DataFrame(risultati)
    df = df.sort_values(by='difficolta_calendario', ascending=False).reset_index(drop=True)
    
    return df

def calcola_confronto(df_reale, df_fantasy_long):
    """Unisce le due classifiche e calcola saldi."""
    cls_reale = calcola_classifica_reale(df_reale)
    cls_fantasy = calcola_classifica_fantasy_da_long(df_fantasy_long)
    
    df_confronto = cls_reale.merge(
        cls_fantasy[['squadra', 'punti_fantasy', 'posizione_fantasy']], 
        on='squadra'
    )
    
    df_confronto['saldo_punti'] = df_confronto['punti_reali'] - df_confronto['punti_fantasy']
    df_confronto['saldo_posizioni'] = df_confronto['posizione_fantasy'] - df_confronto['posizione_reale']
    
    df_confronto = df_confronto.sort_values(
        by=['punti_reali', 'punti_fantasy'], 
        ascending=[False, False]
    ).reset_index(drop=True)
    df_confronto['posizione_finale'] = range(1, len(df_confronto) + 1)
    
    return df_confronto

def genera_round_robin_random(teams):

    teams = teams.copy()
    random.shuffle(teams)

    if len(teams) % 2 == 1:
        teams.append(None)

    n = len(teams)
    rounds = []

    for r in range(n - 1):

        matches = []

        for i in range(n // 2):

            t1 = teams[i]
            t2 = teams[n - 1 - i]

            if t1 and t2:
                matches.append((t1, t2))

        rounds.append(matches)

        teams = [teams[0]] + [teams[-1]] + teams[1:-1]

    random.shuffle(rounds)

    return rounds

def montecarlo_calendari(df_reale, n_sim=10000, salva_prima_sim=False):
    import numpy as np
    import random
    import math

    classifica_reale = calcola_classifica_reale(df_reale)

    squadre = sorted(set(df_reale['squadra1']).union(df_reale['squadra2']))
    N = len(squadre)
    squad_index = {s: i for i, s in enumerate(squadre)}
    idx_to_squad = {i: s for s, i in squad_index.items()}

    # NORMALIZZA GIORNATE
    df_reale = df_reale.copy()
    min_g = df_reale['num_giornata'].min()
    df_reale['num_giornata'] -= (min_g - 1)

    max_giornata = int(df_reale['num_giornata'].max())
    round_robin = N - 1
    
    # Calcola fino a quale multiplo di (N-1) andare
    X = math.ceil(max_giornata / round_robin) * round_robin
    blocchi = X // round_robin

    # =========================
    # MATRICE GOL
    # =========================
    gol_matrix = np.zeros((max_giornata + 1, N))

    for _, r in df_reale.iterrows():
        g = int(r['num_giornata'])
        gol_matrix[g, squad_index[r['squadra1']]] = r['gol1']
        gol_matrix[g, squad_index[r['squadra2']]] = r['gol2']

    # =========================
    # TIEBREAK
    # =========================
    punteggi_tot = np.zeros(N)
    for _, r in df_reale.iterrows():
        i = squad_index[r['squadra1']]
        j = squad_index[r['squadra2']]
        p1 = float(r['punteggio1']) if pd.notna(r['punteggio1']) else 0
        p2 = float(r['punteggio2']) if pd.notna(r['punteggio2']) else 0
        punteggi_tot[i] += p1
        punteggi_tot[j] += p2

    # =========================
    # PREALLOC RISULTATI
    # =========================
    punti_mc = np.zeros((n_sim, N))
    posizioni = np.zeros((n_sim, N))
    vittorie = np.zeros(N)
    
    prima_simulazione = None
    rows_debug = [] if salva_prima_sim else None

    # =========================
    # GENERATORE FAST ROUND ROBIN (INDICI)
    # =========================
    def fast_round_robin_idx():
        """Genera un round-robin random usando solo indici numerici"""
        teams = list(range(N))
        random.shuffle(teams)
        
        if N % 2 == 1:
            teams.append(-1)
            n = N + 1
        else:
            n = N
        
        rounds = []
        for _ in range(n - 1):
            pairs = []
            for i in range(n // 2):
                t1 = teams[i]
                t2 = teams[n - 1 - i]
                if t1 != -1 and t2 != -1:
                    pairs.append((t1, t2))
            rounds.append(pairs)
            # Rotazione: primo fisso, ultimo diventa secondo, il resto shifta
            teams = [teams[0]] + [teams[-1]] + teams[1:-1]
        
        random.shuffle(rounds)  # Mescola l'ordine delle giornate
        return rounds

    # =========================
    # MONTECARLO OTTIMIZZATO
    # =========================
    for sim in range(n_sim):
        punti = np.zeros(N)
        giornata = 1

        # Per ogni blocco, genera nuovo calendario random
        for _ in range(blocchi):
            rr = fast_round_robin_idx()

            for matches in rr:
                if giornata > max_giornata:
                    break

                # Estrazione vettorizzata dei gol per questa giornata
                g1_all = gol_matrix[giornata, :]
                
                for i, j in matches:
                    g1 = g1_all[i]
                    g2 = g1_all[j]

                    # Calcolo punti con operazioni vettorizzate
                    diff = g1 - g2
                    if diff > 0:
                        punti[i] += 3
                    elif diff < 0:
                        punti[j] += 3
                    else:
                        punti[i] += 1
                        punti[j] += 1

                    if salva_prima_sim and sim == 0:
                        rows_debug.append({
                            "num_giornata": giornata,
                            "squadra1": idx_to_squad[i],
                            "squadra2": idx_to_squad[j],
                            "gol1": g1,
                            "gol2": g2
                        })

                giornata += 1

        punti_mc[sim] = punti

        # Ranking argsort doppio (più veloce di sort)
        rank = (-punti).argsort().argsort() + 1
        posizioni[sim] = rank

        # Vincitore con tiebreak
        max_punti = np.max(punti)
        candidate = np.where(punti == max_punti)[0]
        
        if len(candidate) > 1:
            # Tiebreak: massimo punteggio totale (fanta) nei confronti diretti
            winner = candidate[np.argmax(punteggi_tot[candidate])]
        else:
            winner = candidate[0]

        vittorie[winner] += 1

        if salva_prima_sim and sim == 0:
            prima_simulazione = pd.DataFrame(rows_debug)

    # =========================
    # RISULTATI FINALI
    # =========================
    risultati = []
    punti_reali_dict = dict(zip(classifica_reale['squadra'], classifica_reale['punti_reali']))
    pos_reali_dict = dict(zip(classifica_reale['squadra'], classifica_reale['posizione_reale']))

    for squadra in squadre:
        idx = squad_index[squadra]
        distribuzione = punti_mc[:, idx]

        media = np.mean(distribuzione)
        punti_reali = punti_reali_dict.get(squadra, 0)
        pos_reale = pos_reali_dict.get(squadra, 0)

        percentile = np.mean(distribuzione <= punti_reali) * 100
        win_mc = (vittorie[idx] / n_sim) * 100
        pos_media = np.mean(posizioni[:, idx])
        prob_peggio = np.mean(posizioni[:, idx] >= pos_reale) * 100

        risultati.append({
            "squadra": squadra,
            "media_punti_mc": round(float(media), 2),
            "percentile_mc": round(float(percentile), 2),
            "win_mc": round(float(win_mc), 2),
            "posizione_media_mc": round(float(pos_media), 2),
            "prob_peggio": round(float(prob_peggio), 2)
        })

    df_ris = pd.DataFrame(risultati)

    if salva_prima_sim:
        return df_ris, prima_simulazione
    return df_ris
        
def inverti_calendario_sas_style(calendario, squadra_a, squadra_b):
    """
    Algoritmo SAS per inversione calendario:
    1. Crea squadra_sel1 e squadra_sel2 con i dati di ciascuna squadra per giornata
    2. Merge in 'selezionate' per avere entrambe le squadre con i loro gol per giornata
    3. 4 left join per sostituire squadre e gol nel calendario originale
    """
    
    # Step 1: Crea squadra_sel1 (dati di squadra_a)
    squadra_sel1 = []
    for _, row in calendario.iterrows():
        if row['squadra1'] == squadra_a:
            squadra_sel1.append({
                'giornata': row['num_giornata'],
                'team1': row['squadra1'],
                'reti1': row['gol1']
            })
        elif row['squadra2'] == squadra_a:
            squadra_sel1.append({
                'giornata': row['num_giornata'],
                'team1': row['squadra2'],
                'reti1': row['gol2']
            })
    squadra_sel1 = pd.DataFrame(squadra_sel1)
    
    # Step 2: Crea squadra_sel2 (dati di squadra_b)
    squadra_sel2 = []
    for _, row in calendario.iterrows():
        if row['squadra1'] == squadra_b:
            squadra_sel2.append({
                'giornata': row['num_giornata'],
                'team2': row['squadra1'],
                'reti2': row['gol1']
            })
        elif row['squadra2'] == squadra_b:
            squadra_sel2.append({
                'giornata': row['num_giornata'],
                'team2': row['squadra2'],
                'reti2': row['gol2']
            })
    squadra_sel2 = pd.DataFrame(squadra_sel2)
    
    # Step 3: Merge in selezionate (equivalente a merge squadra_sel: in SAS)
    selezionate = pd.merge(squadra_sel1, squadra_sel2, on='giornata', how='outer')
    
    # Step 4: 4 left join per creare calendario_invertito
    result = []
    for _, row in calendario.iterrows():
        num_giornata = row['num_giornata']
        s1, s2 = row['squadra1'], row['squadra2']
        g1, g2 = row['gol1'], row['gol2']
        
        # Trova i dati in selezionate per questa giornata
        sel = selezionate[selezionate['giornata'] == num_giornata]
        if len(sel) == 0:
            # Nessuna delle due squadre in questa giornata, copia così com'è
            result.append({
                'num_giornata': num_giornata,
                'squadra1': new_s1,
                'squadra2': new_s2,
                'gol1': new_g1,
                'gol2': new_g2,
                'punteggio1': 0,
                'punteggio2': 0
            })
            continue
            
        sel = sel.iloc[0]
        
        # Caso b11: squadra1 è team1 (squadra_a) → diventa team2 (squadra_b) con reti2
        if s1 == sel.get('team1'):
            new_s1 = sel.get('team2')
            new_g1 = sel.get('reti2')
        # Caso b12: squadra1 è team2 (squadra_b) → diventa team1 (squadra_a) con reti1
        elif s1 == sel.get('team2'):
            new_s1 = sel.get('team1')
            new_g1 = sel.get('reti1')
        else:
            new_s1 = s1
            new_g1 = g1
        
        # Caso b21: squadra2 è team1 (squadra_a) → diventa team2 (squadra_b) con reti2
        if s2 == sel.get('team1'):
            new_s2 = sel.get('team2')
            new_g2 = sel.get('reti2')
        # Caso b22: squadra2 è team2 (squadra_b) → diventa team1 (squadra_a) con reti1
        elif s2 == sel.get('team2'):
            new_s2 = sel.get('team1')
            new_g2 = sel.get('reti1')
        else:
            new_s2 = s2
            new_g2 = g2
        
        result.append({
            'num_giornata': num_giornata,
            'squadra1': new_s1,
            'squadra2': new_s2,
            'gol1': new_g1,
            'gol2': new_g2,
            'punteggio1': 0,
            'punteggio2': 0
        })
    
    return pd.DataFrame(result)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload_file():
    global ultimo_calendario_reale, ultime_squadre, nome_lega
    
    if 'file' not in request.files:
        return jsonify({'success': False, 'message': 'Nessun file selezionato'})
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({'success': False, 'message': 'Nessun file selezionato'})
    
    if file and allowed_file(file.filename):
        try:
            filename = secure_filename(file.filename)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{timestamp}_{filename}"
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            
            # 1. Parsing calendario reale
            df_grezzo = pd.read_excel(filepath, header=None)
            
            # Estrai nome lega
            nome_lega = estrai_nome_lega(df_grezzo)
            
            df_calendario_reale = parse_calendario(df_grezzo)
            
            # Salva in memoria per uso futuro
            ultimo_calendario_reale = df_calendario_reale.copy()
            ultime_squadre = sorted(df_calendario_reale['squadra1'].unique().tolist() + 
                                   df_calendario_reale['squadra2'].unique().tolist())
            ultime_squadre = sorted(list(set(ultime_squadre)))
            
            # 2. Crea dataset FANTASY LONG
            df_fantasy_long = crea_fantasy_long(df_calendario_reale)
            
            # 3. Calcola classifiche
            df_confronto = calcola_confronto(df_calendario_reale, df_fantasy_long)
            
            # Adatta n_sim in base al numero di squadre
            n_squadre = len(ultime_squadre)
            if n_squadre <= 12:
                n_sim = 10000
            elif n_squadre <= 16:
                n_sim = 1000
            else:
                n_sim = 500  # Per 20 squadre, meno simulazioni ma ancora robusto

            df_mc, prima_sim = montecarlo_calendari(
                df_calendario_reale,
                salva_prima_sim=True
            )

            df_confronto = df_confronto.merge(df_mc, on='squadra', how='left')

            df_confronto['delta_pos_mc'] = (
                df_confronto['posizione_reale'] - df_confronto['posizione_media_mc']
            )
            df_forza_avversari = calcola_forza_avversari(df_calendario_reale)

            # 🔧 FIX NaN per JSON
            df_confronto = df_confronto.replace([float("inf"), -float("inf")], 0)
            df_confronto = df_confronto.fillna(0)
            df_forza_avversari = df_forza_avversari.fillna(0)

            # Stats
            giornate_totali = int(df_calendario_reale['num_giornata'].nunique())
            num_squadre = len(ultime_squadre)
            righe_fantasy = len(df_fantasy_long)
            
            all_scores = pd.concat([
                df_calendario_reale['punteggio1'],
                df_calendario_reale['punteggio2']
            ]).dropna()

            all_scores_non_zero = all_scores[all_scores > 0]

            # levo gli 0 (presunti tavolini) dalla media punti
            media_punti = round(all_scores_non_zero.mean(), 2) if len(all_scores_non_zero) else 0
            
            # Trova più/smeno fortunati
            piu_fortunato = df_confronto.loc[df_confronto['delta_pos_mc'].idxmax()]
            piu_sfortunato = df_confronto.loc[df_confronto['delta_pos_mc'].idxmin()]
            
            # Trova più/smeno fortunati per posizioni
            piu_fortunato_pos = df_confronto.loc[df_confronto['saldo_posizioni'].idxmax()]
            piu_sfortunato_pos = df_confronto.loc[df_confronto['saldo_posizioni'].idxmin()]
            
            # Trova vero campione (primo per punti fantasy)
            vero_campione = df_confronto.loc[df_confronto['win_mc'].idxmax()]
            
            # Salva file
            output_reale = f"calendario_reale_{timestamp}.xlsx"
            output_fantasy = f"calendario_fantasy_{timestamp}.xlsx"
            output_confronto = f"confronto_classifiche_{timestamp}.xlsx"
            output_forza = f"forza_avversari_{timestamp}.xlsx"
            output_sim = f"prima_simulazione_mc_{timestamp}.xlsx"
            
            df_fantasy_export = df_fantasy_long[[
                'squadra', 'avversario', 'giornata', 'punteggio', 'punti_ottenuti'
            ]].copy()
            
            df_calendario_reale.to_excel(os.path.join(app.config['UPLOAD_FOLDER'], output_reale), index=False)
            df_fantasy_export.to_excel(os.path.join(app.config['UPLOAD_FOLDER'], output_fantasy), index=False)
            df_confronto.to_excel(os.path.join(app.config['UPLOAD_FOLDER'], output_confronto), index=False)
            df_forza_avversari.to_excel(os.path.join(app.config['UPLOAD_FOLDER'], output_forza), index=False)
            prima_sim.to_excel(
                os.path.join(app.config['UPLOAD_FOLDER'], output_sim),
                index=False
            )

            stats = {
                'nome_lega': nome_lega,
                'squadre_totali': len(df_confronto),
                'giornate_totali': giornate_totali,
                'partite_reali_totali': len(df_calendario_reale),
                'righe_fantasy_long': righe_fantasy,
                'partite_per_squadra': int(num_squadre - 1),
                'media_punti': media_punti,
                'elenco_squadre': ultime_squadre,
                'piu_fortunato': {
                    'squadra': piu_fortunato['squadra'],
                    'saldo': round(piu_fortunato['delta_pos_mc'], 2)
                },
                'piu_sfortunato': {
                    'squadra': piu_sfortunato['squadra'],
                    'saldo': round(piu_sfortunato['delta_pos_mc'], 2)
                },
                'piu_fortunato_pos': {
                    'squadra': piu_fortunato_pos['squadra'],
                    'saldo': abs(int(piu_fortunato_pos['saldo_posizioni']))
                },
                'piu_sfortunato_pos': {
                    'squadra': piu_sfortunato_pos['squadra'],
                    'saldo': abs(int(piu_sfortunato_pos['saldo_posizioni']))
                },
                'vero_campione': {
                    'squadra': vero_campione['squadra'],
                    'punti': round(vero_campione['media_punti_mc'], 2)
                },
                'confronto': df_confronto.to_dict('records'),
                'forza_avversari': df_forza_avversari.to_dict('records')
            }
            
            return jsonify({
                'success': True,
                'message': f'{righe_fantasy} righe fantasy',
                'filename': filename,
                'output_reale': output_reale,
                'output_fantasy': output_fantasy,
                'output_confronto': output_confronto,
                'output_forza': output_forza,
                'output_simulazione': output_sim,
                'stats': stats
            })
            
        except Exception as e:
            import traceback
            print(traceback.format_exc())
            return jsonify({
                'success': False,
                'message': f'Errore: {str(e)}',
                'dettaglio': traceback.format_exc()
            })
    
    return jsonify({'success': False, 'message': 'Formato non valido'})


@app.route('/inverti', methods=['POST'])
def inverti():
    global ultimo_calendario_reale, ultime_squadre
    
    if ultimo_calendario_reale is None:
        return jsonify({'success': False, 'message': 'Nessun calendario caricato. Carica prima un file.'})
    
    data = request.get_json()
    squadra_a = data.get('squadra_a', '').upper().strip()
    squadra_b = data.get('squadra_b', '').upper().strip()
    
    if not squadra_a or not squadra_b:
        return jsonify({'success': False, 'message': 'Seleziona entrambe le squadre'})
    
    if squadra_a == squadra_b:
        return jsonify({'success': False, 'message': 'Seleziona due squadre diverse'})
    
    if squadra_a not in ultime_squadre or squadra_b not in ultime_squadre:
        return jsonify({'success': False, 'message': f'Squadre non trovate. Disponibili: {", ".join(ultime_squadre)}'})
    
    try:
        # Calcola classifica originale
        classifica_originale = calcola_classifica_reale(ultimo_calendario_reale)
        punti_originali = dict(zip(classifica_originale['squadra'], classifica_originale['punti_reali']))
        pos_originali = dict(zip(classifica_originale['squadra'], classifica_originale['posizione_reale']))
        
        # Inverti calendario usando algoritmo SAS
        df_invertito = inverti_calendario_sas_style(ultimo_calendario_reale, squadra_a, squadra_b)
        
        # Ricalcola classifica reale con calendario invertito
        classifica_invertita = calcola_classifica_reale(df_invertito)
        
        # Prepara risultato con confronto
        risultato = []
        for _, row in classifica_invertita.iterrows():
            squadra = row['squadra']
            punti_inv = row['punti_reali']
            punti_orig = punti_originali.get(squadra, 0)
            pos_inv = row['posizione_reale']
            pos_orig = pos_originali.get(squadra, 0)
            
            risultato.append({
                'squadra': squadra,
                'punti_originali': punti_orig,
                'punti_invertiti': punti_inv,
                'saldo_punti': punti_inv - punti_orig,
                'posizione_originale': pos_orig,
                'posizione_invertita': pos_inv,
                'saldo_posizioni': pos_orig - pos_inv
            })
        
        return jsonify({
            'success': True,
            'message': f'Calendario invertito: {squadra_a} ↔ {squadra_b}',
            'squadra_a': squadra_a,
            'squadra_b': squadra_b,
            'classifica': risultato
        })
        
    except Exception as e:
        import traceback
        return jsonify({
            'success': False,
            'message': f'Errore: {str(e)}',
            'dettaglio': traceback.format_exc()
        })


@app.route('/uploads/<filename>')
def download_file(filename):
    return send_file(os.path.join(app.config['UPLOAD_FOLDER'], filename), as_attachment=True)


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)