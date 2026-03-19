import requests
import pandas as pd
import numpy as np
import time
import schedule
import psycopg2
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

ARG_TZ = ZoneInfo('America/Argentina/Buenos_Aires')

TELEGRAM_TOKEN = "8621482285:AAFXlOcgNwRQp1MMmYABaDLUXrXAoQgDplc"
CHAT_ID        = "1343270628"
BINANCE_BASE   = "https://api.binance.com/api/v3"
MIN_SCORE      = 3
RSI_OVERBOUGHT = 70
RSI_OVERSOLD   = 30
RSI_EXTREME    = 75
ALERTA_COOLDOWN_MIN = 30  # no repetir la misma alerta por 30 minutos

SYMBOLS = [
    "BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","BNBUSDT",
    "DOGEUSDT","ADAUSDT","AVAXUSDT","DOTUSDT","MATICUSDT",
    "LINKUSDT","LTCUSDT","ATOMUSDT","NEARUSDT","HBARUSDT",
    "THETAUSDT","FTMUSDT","SANDUSDT","MANAUSDT","INJUSDT"
]

INTERVALS = [("15m","Scalping 15m"),("1h","Day Trading 1H")]

# ============================================================
#   BASE DE DATOS — MEMORIA
# ============================================================
def get_db():
    try:
        return psycopg2.connect(os.environ["DATABASE_URL"])
    except Exception as e:
        print("Error DB: " + str(e))
        return None

def init_db():
    conn = get_db()
    if not conn: return
    try:
        cur = conn.cursor()
        # Tabla de alertas enviadas
        cur.execute("""
            CREATE TABLE IF NOT EXISTS alertas (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20),
                direction VARCHAR(10),
                timeframe VARCHAR(20),
                precio FLOAT,
                rsi FLOAT,
                score FLOAT,
                sl FLOAT,
                tp1 FLOAT,
                tp2 FLOAT,
                tp3 FLOAT,
                confluencias TEXT,
                enviada_at TIMESTAMP DEFAULT NOW()
            )
        """)
        # Tabla de estado del mercado
        cur.execute("""
            CREATE TABLE IF NOT EXISTS market_state (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20),
                timeframe VARCHAR(20),
                rsi FLOAT,
                estructura TEXT,
                htf_bias TEXT,
                divergencia TEXT,
                precio FLOAT,
                actualizado_at TIMESTAMP DEFAULT NOW()
            )
        """)
        # Tabla de RSI historico para divergencias entre escaneos
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rsi_history (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20),
                timeframe VARCHAR(20),
                rsi FLOAT,
                precio FLOAT,
                registrado_at TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()
        # Ampliar campos VARCHAR por si las tablas ya existían con tamaño menor
        try:
            cur.execute("ALTER TABLE alertas ALTER COLUMN timeframe TYPE VARCHAR(20)")
            cur.execute("ALTER TABLE market_state ALTER COLUMN timeframe TYPE VARCHAR(20)")
            cur.execute("ALTER TABLE rsi_history ALTER COLUMN timeframe TYPE VARCHAR(20)")
            conn.commit()
        except:
            conn.rollback()
        print("DB iniciada OK")
    except Exception as e:
        print("Error init DB: " + str(e))
    finally:
        conn.close()

def ya_alerte(symbol, direction, timeframe):
    """Verifica si ya se mandó alerta de este setup en los últimos COOLDOWN minutos"""
    conn = get_db()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM alertas
            WHERE symbol = %s AND direction = %s AND timeframe = %s
            AND enviada_at > NOW() - (INTERVAL '1 minute' * %s)
        """, (symbol, direction, timeframe, ALERTA_COOLDOWN_MIN))
        count = cur.fetchone()[0]
        return count > 0
    except:
        return False
    finally:
        conn.close()

def guardar_alerta(s, tf_label):
    """Guarda la alerta en la base de datos"""
    conn = get_db()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO alertas (symbol, direction, timeframe, precio, rsi, score, sl, tp1, tp2, tp3, confluencias)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            s["symbol"], s["direction"], tf_label,
            float(s["price"]), float(s["rsi"]), float(s["score"]),
            float(s["sl"]), float(s["tp1"]), float(s["tp2"]), float(s["tp3"]),
            " | ".join(s["labels"])
        ))
        conn.commit()
    except Exception as e:
        print("Error guardar alerta: " + str(e))
    finally:
        conn.close()

def guardar_estado(symbol, timeframe, rsi, estructura, htf_bias, divergencia, precio):
    """Guarda el estado actual del mercado"""
    conn = get_db()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO market_state (symbol, timeframe, rsi, estructura, htf_bias, divergencia, precio)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (symbol, timeframe, float(rsi), str(estructura), str(htf_bias), str(divergencia), float(precio)))
        conn.commit()
    except Exception as e:
        print("Error guardar estado: " + str(e))
    finally:
        conn.close()

def guardar_rsi(symbol, timeframe, rsi, precio):
    """Guarda RSI historico para detectar divergencias entre escaneos"""
    conn = get_db()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO rsi_history (symbol, timeframe, rsi, precio)
            VALUES (%s, %s, %s, %s)
        """, (symbol, timeframe, float(rsi), float(precio)))
        cur.execute("""
            DELETE FROM rsi_history
            WHERE id NOT IN (
                SELECT id FROM rsi_history
                WHERE symbol = %s AND timeframe = %s
                ORDER BY registrado_at DESC
                LIMIT 200
            ) AND symbol = %s AND timeframe = %s
        """, (symbol, timeframe, symbol, timeframe))
        conn.commit()
    except Exception as e:
        print("Error guardar RSI: " + str(e))
    finally:
        conn.close()

def get_historial_alertas(limite=10):
    """Obtiene las últimas alertas enviadas"""
    conn = get_db()
    if not conn: return []
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT symbol, direction, timeframe, precio, score, enviada_at
            FROM alertas
            ORDER BY enviada_at DESC
            LIMIT %s
        """, (limite,))
        return cur.fetchall()
    except:
        return []
    finally:
        conn.close()

# ============================================================
#   TELEGRAM
# ============================================================
def send_telegram(message):
    url = "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/sendMessage"
    for chunk in [message[i:i+4000] for i in range(0, len(message), 4000)]:
        try:
            requests.post(url, json={"chat_id": CHAT_ID, "text": chunk, "parse_mode": "HTML"}, timeout=10)
            time.sleep(0.5)
        except Exception as e:
            print("Error Telegram: " + str(e))

# ============================================================
#   DATOS
# ============================================================
def get_klines(symbol, interval, limit=200):
    try:
        r = requests.get(BINANCE_BASE + "/klines",
                         params={"symbol": symbol, "interval": interval, "limit": limit},
                         timeout=10)
        data = r.json()
        if not isinstance(data, list) or len(data) < 10:
            return None
        df = pd.DataFrame(data, columns=[
            "time","open","high","low","close","volume",
            "close_time","quote_vol","trades","taker_base","taker_quote","ignore"
        ])
        for col in ["open","high","low","close","volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna().reset_index(drop=True)
        return df if len(df) >= 30 else None
    except:
        return None

def fmt(p):
    try:
        if p >= 100:  return "$" + "{:,.2f}".format(p)
        elif p >= 1:  return "$" + "{:.4f}".format(p)
        else:         return "$" + "{:.6f}".format(p)
    except:
        return str(p)

def calc_rsi(closes, period=14):
    try:
        delta = closes.diff()
        gain  = delta.where(delta > 0, 0.0)
        loss  = (-delta.where(delta < 0, 0.0))
        # Media de Wilder (EMA con alpha=1/period) — estándar para RSI
        avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        rs  = avg_gain / avg_loss
        val = (100 - 100 / (1 + rs)).iloc[-1]
        return round(val, 2) if not np.isnan(val) else 50.0
    except:
        return 50.0

def detect_rsi_divergence(df, period=14):
    try:
        closes = df["close"]
        delta  = closes.diff()
        gain   = delta.where(delta > 0, 0.0)
        loss   = (-delta.where(delta < 0, 0.0))
        avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        rsi_full = 100 - 100 / (1 + avg_gain / avg_loss)
        lookback   = 100
        recent_df  = df.iloc[-lookback:]
        recent_rsi = rsi_full.iloc[-lookback:]
        price_highs, rsi_highs = [], []
        price_lows,  rsi_lows  = [], []
        for i in range(3, len(recent_df) - 3):
            ph = recent_df["high"].iloc[i]
            pl = recent_df["low"].iloc[i]
            rh = recent_rsi.iloc[i]
            if (ph > recent_df["high"].iloc[i-1] and ph > recent_df["high"].iloc[i+1] and
                ph > recent_df["high"].iloc[i-2] and ph > recent_df["high"].iloc[i+2]):
                price_highs.append((i, ph)); rsi_highs.append((i, rh))
            if (pl < recent_df["low"].iloc[i-1] and pl < recent_df["low"].iloc[i+1] and
                pl < recent_df["low"].iloc[i-2] and pl < recent_df["low"].iloc[i+2]):
                price_lows.append((i, pl)); rsi_lows.append((i, rh))
        divergence = None
        if len(price_highs) >= 2 and price_highs[-1][1] > price_highs[-2][1] and rsi_highs[-1][1] < rsi_highs[-2][1] and rsi_highs[-1][1] > 50:
            divergence = "BEARISH_DIV"
        if len(price_lows) >= 2 and price_lows[-1][1] < price_lows[-2][1] and rsi_lows[-1][1] > rsi_lows[-2][1] and rsi_lows[-1][1] < 50:
            divergence = "BULLISH_DIV"
        return divergence
    except:
        return None

def get_htf_bias(symbol, interval):
    try:
        next_tf = "4h" if interval in ("15m","1h") else "1d"
        df = get_klines(symbol, next_tf, limit=250)  # 250 velas para EMA200 preciso
        if df is None: return "NEUTRAL"
        closes = df["close"]
        price  = closes.iloc[-1]
        ema50  = closes.ewm(span=50,  adjust=False, min_periods=50).mean().iloc[-1]
        ema200 = closes.ewm(span=200, adjust=False, min_periods=200).mean().iloc[-1]
        if np.isnan(ema50) or np.isnan(ema200): return "NEUTRAL"
        bull, bear = 0, 0
        if price > ema50:  bull += 1
        else:              bear += 1
        if price > ema200: bull += 1
        else:              bear += 1
        if ema50 > ema200: bull += 1
        else:              bear += 1
        highs, lows = [], []
        for i in range(2, len(df) - 2):
            if df["high"].iloc[i] > df["high"].iloc[i-1] and df["high"].iloc[i] > df["high"].iloc[i+1]:
                highs.append(df["high"].iloc[i])
            if df["low"].iloc[i] < df["low"].iloc[i-1] and df["low"].iloc[i] < df["low"].iloc[i+1]:
                lows.append(df["low"].iloc[i])
        if len(highs) >= 2 and len(lows) >= 2:
            if highs[-1] > highs[-2] and lows[-1] > lows[-2]:   bull += 2
            elif highs[-1] < highs[-2] and lows[-1] < lows[-2]: bear += 2
        if bull > bear:   return "BULLISH"
        elif bear > bull: return "BEARISH"
        return "NEUTRAL"
    except:
        return "NEUTRAL"

def calc_sr(df):
    try:
        price = df["close"].iloc[-1]
        ph, pl = [], []
        for i in range(3, len(df) - 3):
            w = df["high"].iloc[i-3:i+4]
            if len(w) == 7 and df["high"].iloc[i] == w.max(): ph.append(df["high"].iloc[i])
            w2 = df["low"].iloc[i-3:i+4]
            if len(w2) == 7 and df["low"].iloc[i] == w2.min(): pl.append(df["low"].iloc[i])
        res = sorted([h for h in ph if h > price])
        sup = sorted([l for l in pl if l < price], reverse=True)
        return (round(sup[0],6) if sup else round(price*0.98,6)), (round(res[0],6) if res else round(price*1.02,6))
    except:
        price = df["close"].iloc[-1]
        return round(price*0.98,6), round(price*1.02,6)

def detect_ob(df):
    ob_bull, ob_bear = None, None
    try:
        price = df["close"].iloc[-1]
        # Buscar desde el más reciente hacia atrás — quedarse con el OB más cercano al precio
        for i in range(min(len(df)-3, 200), 4, -1):
            c = df.iloc[i]
            body = abs(c["close"] - c["open"])
            rng  = c["high"] - c["low"]
            if rng == 0: continue
            ratio = body / rng
            end = min(i+10, len(df))

            # OB ALCISTA — vela bajista seguida de impulso alcista
            # El precio debe estar DENTRO o JUSTO ENCIMA del OB (no más de 0.5% arriba)
            if ob_bull is None and c["close"] < c["open"] and ratio > 0.55:
                fh = df["high"].iloc[i+1:end].max()
                if fh > c["high"]:
                    # Precio debe estar entre el low del OB y un 0.5% encima del high
                    if c["low"] <= price <= c["high"] * 1.005:
                        ob_bull = {"high": round(c["high"],6), "low": round(c["low"],6)}

            # OB BAJISTA — vela alcista seguida de impulso bajista
            # El precio debe estar DENTRO o JUSTO DEBAJO del OB (no más de 0.5% abajo)
            if ob_bear is None and c["close"] > c["open"] and ratio > 0.55:
                fl = df["low"].iloc[i+1:end].min()
                if fl < c["low"]:
                    # Precio debe estar entre el high del OB y un 0.5% debajo del low
                    if c["low"] * 0.995 <= price <= c["high"]:
                        ob_bear = {"high": round(c["high"],6), "low": round(c["low"],6)}

            # Si ya encontró ambos, terminar
            if ob_bull and ob_bear:
                break
    except:
        pass
    return ob_bull, ob_bear

def detect_fvg(df):
    fvg_bull, fvg_bear = None, None
    try:
        price = df["close"].iloc[-1]
        search_df = df.iloc[-100:]
        # Buscar desde el más reciente hacia atrás
        for i in range(len(search_df)-1, 1, -1):
            # FVG ALCISTA — precio debe estar DENTRO del gap
            if fvg_bull is None and search_df["low"].iloc[i] > search_df["high"].iloc[i-2]:
                gl, gh = search_df["high"].iloc[i-2], search_df["low"].iloc[i]
                gap_size = (gh - gl) / price * 100
                if gap_size > 0.05 and gl <= price <= gh:
                    fvg_bull = {"high": round(gh,6), "low": round(gl,6), "size": round(gap_size,3)}
            # FVG BAJISTA — precio debe estar DENTRO del gap
            if fvg_bear is None and search_df["high"].iloc[i] < search_df["low"].iloc[i-2]:
                gh, gl = search_df["low"].iloc[i-2], search_df["high"].iloc[i]
                gap_size = (gh - gl) / price * 100
                if gap_size > 0.05 and gl <= price <= gh:
                    fvg_bear = {"high": round(gh,6), "low": round(gl,6), "size": round(gap_size,3)}
            if fvg_bull and fvg_bear:
                break
    except:
        pass
    return fvg_bull, fvg_bear

def detect_structure(df):
    try:
        highs, lows = [], []
        for i in range(2, len(df)-2):
            if df["high"].iloc[i] > df["high"].iloc[i-1] and df["high"].iloc[i] > df["high"].iloc[i+1]:
                highs.append((i, df["high"].iloc[i]))
            if df["low"].iloc[i] < df["low"].iloc[i-1] and df["low"].iloc[i] < df["low"].iloc[i+1]:
                lows.append((i, df["low"].iloc[i]))
        if len(highs) < 2 or len(lows) < 2: return None
        lh, ph = highs[-1][1], highs[-2][1]
        ll, pl = lows[-1][1],  lows[-2][1]
        lc, pc = df["close"].iloc[-1], df["close"].iloc[-2]
        # BOS/CHoCH requiere cierre confirmado por encima/debajo del nivel
        # y que el nivel sea reciente (últimas 50 velas)
        last_high_idx = highs[-1][0]
        last_low_idx  = lows[-1][0]
        if lc > lh and pc <= lh and (len(df) - last_high_idx) <= 50:
            return "BOS_BULL" if lh > ph else "CHoCH_BULL"
        if lc < ll and pc >= ll and (len(df) - last_low_idx) <= 50:
            return "BOS_BEAR" if ll < pl else "CHoCH_BEAR"
    except:
        pass
    return None

def detect_hh_ll(df, lookback=50):
    """
    Detecta si el precio actual está cerca de un HH o LL reciente.
    HH = Higher High — zona de oferta, ideal para SHORT
    LL = Lower Low  — zona de demanda, ideal para LONG
    Retorna: ("HH", nivel) | ("LL", nivel) | None
    Tolerancia: precio dentro del 0.8% del HH o LL
    """
    try:
        price    = df["close"].iloc[-1]
        recent   = df.iloc[-lookback:]
        highs, lows = [], []
        for i in range(2, len(recent)-2):
            if (recent["high"].iloc[i] > recent["high"].iloc[i-1] and
                recent["high"].iloc[i] > recent["high"].iloc[i+1] and
                recent["high"].iloc[i] > recent["high"].iloc[i-2] and
                recent["high"].iloc[i] > recent["high"].iloc[i+2]):
                highs.append((i, recent["high"].iloc[i]))
            if (recent["low"].iloc[i] < recent["low"].iloc[i-1] and
                recent["low"].iloc[i] < recent["low"].iloc[i+1] and
                recent["low"].iloc[i] < recent["low"].iloc[i-2] and
                recent["low"].iloc[i] < recent["low"].iloc[i+2]):
                lows.append((i, recent["low"].iloc[i]))

        # HH — el último high es mayor que el anterior
        if len(highs) >= 2:
            last_h, prev_h = highs[-1][1], highs[-2][1]
            if last_h > prev_h:
                # Precio cerca del HH (dentro del 0.8%)
                if abs(price - last_h) / price <= 0.008:
                    return "HH", round(last_h, 6)

        # LL — el último low es menor que el anterior
        if len(lows) >= 2:
            last_l, prev_l = lows[-1][1], lows[-2][1]
            if last_l < prev_l:
                # Precio cerca del LL (dentro del 0.8%)
                if abs(price - last_l) / price <= 0.008:
                    return "LL", round(last_l, 6)
    except:
        pass
    return None, None

def detect_candle(df):
    patterns = []
    try:
        c, p = df.iloc[-1], df.iloc[-2]
        body  = abs(c["close"] - c["open"])
        total = c["high"] - c["low"]
        if total == 0: return patterns
        upper = c["high"] - max(c["close"], c["open"])
        lower = min(c["close"], c["open"]) - c["low"]
        ratio = body / total
        if ratio < 0.1: patterns.append("Doji")
        if upper > body*2 and upper > lower*2: patterns.append("Pin bar bajista")
        if lower > body*2 and lower > upper*2: patterns.append("Pin bar alcista")
        if (c["close"] < c["open"] and p["close"] > p["open"] and
            c["open"] >= p["close"] and c["close"] <= p["open"]): patterns.append("Engulfing bajista")
        if (c["close"] > c["open"] and p["close"] < p["open"] and
            c["open"] <= p["close"] and c["close"] >= p["open"]): patterns.append("Engulfing alcista")
    except:
        pass
    return patterns

def calc_vol(df):
    try:
        avg  = df["volume"].iloc[-21:-1].mean()
        last = df["volume"].iloc[-1]
        if avg == 0: return 100, False
        ratio = round((last/avg)*100)
        return ratio, ratio >= 150
    except:
        return 100, False

def calc_volatility(df):
    try:
        if len(df) < 5: return 0.0
        n = min(20, len(df))
        return round(((df["high"]-df["low"])/df["close"]*100).iloc[-n:].mean(), 2)
    except:
        return 0.0

def calc_sl_tp(price, direction, support, resistance):
    if direction == "LONG":
        sl   = round(support * 0.997, 6)
        risk = max(price - sl, price * 0.005)
        tp1, tp2, tp3 = round(price+risk*1.5,6), round(price+risk*2.5,6), round(price+risk*4.0,6)
    else:
        sl   = round(resistance * 1.003, 6)
        risk = max(sl - price, price * 0.005)
        tp1, tp2, tp3 = round(price-risk*1.5,6), round(price-risk*2.5,6), round(price-risk*4.0,6)
    return sl, tp1, tp2, tp3, round(abs(tp1-price)/risk,1), round(abs(tp2-price)/risk,1)

def clasificar_setup(direction, rsi, structure, divergence, htf_bias):
    """
    Clasifica si el setup es una REVERSIÓN real o solo un REBOTE técnico.
    REVERSIÓN: CHoCH confirmado + divergencia RSI + HTF a favor
    REBOTE: RSI extremo pero HTF en contra, sin cambio de estructura
    """
    htf_a_favor = (direction == "LONG" and htf_bias == "BULLISH") or \
                  (direction == "SHORT" and htf_bias == "BEARISH")
    choch = (direction == "LONG" and structure == "CHoCH_BULL") or \
            (direction == "SHORT" and structure == "CHoCH_BEAR")
    bos   = (direction == "LONG" and structure == "BOS_BULL") or \
            (direction == "SHORT" and structure == "BOS_BEAR")
    div_a_favor = (direction == "LONG" and divergence == "BULLISH_DIV") or \
                  (direction == "SHORT" and divergence == "BEARISH_DIV")

    # REVERSIÓN — necesita al menos 2 de 3: CHoCH/BOS, divergencia, HTF a favor
    confirmaciones = sum([choch or bos, div_a_favor, htf_a_favor])
    if confirmaciones >= 2:
        return "REVERSION"
    # REBOTE — RSI extremo pero sin confirmaciones suficientes
    rsi_extremo = (direction == "LONG" and rsi <= (100 - RSI_EXTREME)) or \
                  (direction == "SHORT" and rsi >= RSI_EXTREME)
    if rsi_extremo:
        return "REBOTE"
    return "SETUP"

def calc_score(direction, rsi, ob, fvg, structure, patterns, vol_high, near_sr, divergence, htf_bias):
    score, labels = 0, []
    rsi_extreme_short = direction == "SHORT" and rsi >= RSI_EXTREME
    rsi_extreme_long  = direction == "LONG"  and rsi <= (100 - RSI_EXTREME)
    if direction == "LONG":
        if rsi <= RSI_OVERSOLD:   score += 1.5; labels.append("RSI sobrevendido (" + str(rsi) + ")")
        if ob:    score += 2; labels.append("OB alcista " + fmt(ob["low"]) + "-" + fmt(ob["high"]))
        if fvg:   score += 1.5; labels.append("FVG alcista " + fmt(fvg["low"]) + "-" + fmt(fvg["high"]))
        if structure in ("BOS_BULL","CHoCH_BULL"): score += 1.5; labels.append("Estructura: " + structure)
        bull_c = [p for p in patterns if "alcista" in p or "Doji" in p]
        if bull_c: score += 1; labels.append(" | ".join(bull_c))
        if divergence == "BULLISH_DIV": score += 1.5; labels.append("Divergencia RSI alcista")
        if htf_bias == "BULLISH": score += 2; labels.append("HTF a favor (ALCISTA)")
        elif htf_bias == "BEARISH": score -= 1
    else:
        if rsi >= RSI_OVERBOUGHT: score += 1.5; labels.append("RSI sobrecomprado (" + str(rsi) + ")")
        if ob:    score += 2; labels.append("OB bajista " + fmt(ob["low"]) + "-" + fmt(ob["high"]))
        if fvg:   score += 1.5; labels.append("FVG bajista " + fmt(fvg["low"]) + "-" + fmt(fvg["high"]))
        if structure in ("BOS_BEAR","CHoCH_BEAR"): score += 1.5; labels.append("Estructura: " + structure)
        bear_c = [p for p in patterns if "bajista" in p or "Doji" in p]
        if bear_c: score += 1; labels.append(" | ".join(bear_c))
        if divergence == "BEARISH_DIV": score += 1.5; labels.append("Divergencia RSI bajista")
        if htf_bias == "BEARISH": score += 2; labels.append("HTF a favor (BAJISTA)")
        elif htf_bias == "BULLISH": score -= 1
    if vol_high:  score += 1; labels.append("Volumen elevado")
    if near_sr:   score += 1; labels.append("Precio en S/R clave")
    if rsi_extreme_short or rsi_extreme_long:
        score = max(score, MIN_SCORE)
        if not any("extremo" in l for l in labels):
            labels.insert(0, "RSI extremo (" + str(rsi) + ") — alerta directa")
    return round(min(max(score, 0), 10)), labels

def format_setup(s, tf_label):
    d         = s["direction"]
    is_long   = d == "LONG"
    htf_label = "ALCISTA" if s["htf_bias"] == "BULLISH" else ("BAJISTA" if s["htf_bias"] == "BEARISH" else "NEUTRAL")
    htf_emoji = "🟢" if s["htf_bias"] == "BULLISH" else ("🔴" if s["htf_bias"] == "BEARISH" else "⚪")
    stars     = "⭐" * min(int(s["score"]), 5)
    tipo      = s.get("tipo_setup", "SETUP")

    # Header según tipo de setup
    if tipo == "REVERSION":
        tipo_tag = "🔄 REVERSIÓN"
    elif tipo == "REBOTE":
        tipo_tag = "↩️ REBOTE"
    else:
        tipo_tag = ""

    if is_long:
        header = "🟢 <b>LONG 📈 — " + s["symbol"] + "</b>"
    else:
        header = "🔴 <b>SHORT 📉 — " + s["symbol"] + "</b>"

    lines = [
        "",
        header + " [" + tf_label + "]" + (" | " + tipo_tag if tipo_tag else ""),
        "Score: <b>" + str(s["score"]) + "/10</b> " + stars,
    ]

    # Advertencia especial para rebotes
    if tipo == "REBOTE":
        lines.append("⚠️ <b>REBOTE TÉCNICO — HTF en contra. TP corto, riesgo alto.</b>")

    lines += [
        "💰 Precio actual: " + fmt(s["price"]),
    ]

    lines += [
        "📊 RSI:         " + str(s["rsi"]),
        htf_emoji + " HTF Bias:    " + htf_label,
        "🌊 Volatilidad: " + str(s["volatility"]) + "%",
        "📊 Volumen:     " + str(s["vol_ratio"]) + "% del promedio",
    ]
    if s["divergence"]:
        div_emoji = "📈" if s["divergence"] == "BULLISH_DIV" else "📉"
        lines.append(div_emoji + " Divergencia: " + ("Alcista" if s["divergence"] == "BULLISH_DIV" else "Bajista"))
    if s["ob"]:
        ob_emoji = "🟩" if is_long else "🟥"
        lines.append(ob_emoji + " Order Block: " + fmt(s["ob"]["low"]) + " - " + fmt(s["ob"]["high"]))
    if s["fvg"]:
        sz = s["fvg"].get("size", "")
        lines.append("⬜ FVG:        " + fmt(s["fvg"]["low"]) + " - " + fmt(s["fvg"]["high"]) + (" (" + str(sz) + "%)" if sz else ""))
    if s["structure"]:
        lines.append("🔷 Estructura: " + s["structure"])
    hh_ll_type  = s.get("hh_ll_type")
    hh_ll_level = s.get("hh_ll_level")
    if hh_ll_type == "HH" and hh_ll_level:
        lines.append("🔺 HH detectado: " + fmt(hh_ll_level) + " — zona de oferta ideal para SHORT")
    elif hh_ll_type == "LL" and hh_ll_level:
        lines.append("🔻 LL detectado: " + fmt(hh_ll_level) + " — zona de demanda ideal para LONG")
    lines.append("🕯 Vela:       " + (" | ".join(s["candles"]) if s["candles"] else "Sin patron"))
    lines += [
        "🟢 Soporte:     " + fmt(s["support"]),
        "🔴 Resistencia: " + fmt(s["resistance"]),
        "━━━━━━━━━━━━━━━",
    ]
    # Entrada estimada junto al SL y TPs
    ob  = s.get("ob")
    fvg = s.get("fvg")
    if ob:
        entrada = round((ob["high"] + ob["low"]) / 2, 6)
        lines.append("🟡 Entrada:  " + fmt(entrada) + " (zona OB)")
    elif fvg:
        entrada = round((fvg["high"] + fvg["low"]) / 2, 6)
        lines.append("🟡 Entrada:  " + fmt(entrada) + " (zona FVG)")
    else:
        lines.append("🟡 Entrada:  " + fmt(s["price"]) + " (precio actual)")
    lines += [
        "🛑 SL:      " + fmt(s["sl"]),
        "🎯 TP1:     " + fmt(s["tp1"]) + " (R:R 1:" + str(s["rr1"]) + ")",
        "🎯 TP2:     " + fmt(s["tp2"]) + " (R:R 1:" + str(s["rr2"]) + ")",
        "🎯 TP3:     " + fmt(s["tp3"]),
        "✅ Confluencias:",
    ]
    for l in s["labels"]:
        lines.append("   • " + l)
    return "\n".join(lines)

def analyze_symbol(symbol, interval):
    df = get_klines(symbol, interval, 200)
    if df is None: return None
    try:
        price        = df["close"].iloc[-1]
        rsi          = calc_rsi(df["close"])
        sup, res     = calc_sr(df)
        ob_b, ob_s   = detect_ob(df)
        fv_b, fv_s   = detect_fvg(df)
        structure    = detect_structure(df)
        candles      = detect_candle(df)
        vol_r, vol_h = calc_vol(df)
        vol          = calc_volatility(df)
        divergence   = detect_rsi_divergence(df)
        htf_bias     = get_htf_bias(symbol, interval)
        hh_ll_type, hh_ll_level = detect_hh_ll(df)
        near_sup     = abs(price - sup) / price < 0.005
        near_res     = abs(price - res) / price < 0.005

        # Guardar estado y RSI en DB
        guardar_estado(symbol, interval, rsi, structure, htf_bias, divergence, price)
        guardar_rsi(symbol, interval, rsi, price)

        results = []
        for direction, ob, fvg, near in [("LONG",ob_b,fv_b,near_sup),("SHORT",ob_s,fv_s,near_res)]:
            # Filtro RSI direccional — SHORT solo si RSI>50, LONG solo si RSI<50
            if direction == "SHORT" and rsi < 50 and rsi < RSI_EXTREME: continue
            if direction == "LONG"  and rsi > 50 and rsi > (100-RSI_EXTREME): continue
            # Filtro HTF — no operar contra el bias mayor (salvo RSI extremo)
            rsi_extremo = (direction == "SHORT" and rsi >= RSI_EXTREME) or (direction == "LONG" and rsi <= (100-RSI_EXTREME))
            if direction == "SHORT" and htf_bias == "BULLISH" and not rsi_extremo: continue
            if direction == "LONG"  and htf_bias == "BEARISH" and not rsi_extremo: continue
            score, labels = calc_score(direction, rsi, ob, fvg, structure, candles, vol_h, near, divergence, htf_bias)
            # Clasificar si es reversión o rebote
            tipo_setup = clasificar_setup(direction, rsi, structure, divergence, htf_bias)
            # Techo de score para REBOTE — máximo 5/10
            if tipo_setup == "REBOTE":
                score = min(score, 5)
            # Filtro vela — bloquear si la vela actual contradice la dirección
            velas_bajistas = [p for p in candles if "bajista" in p]
            velas_alcistas = [p for p in candles if "alcista" in p]
            if direction == "LONG"  and velas_bajistas: continue
            if direction == "SHORT" and velas_alcistas: continue
            # Filtro estructura — BOS contrario bloquea la señal
            if direction == "LONG"  and structure in ("BOS_BEAR", "CHoCH_BEAR"): continue
            if direction == "SHORT" and structure in ("BOS_BULL", "CHoCH_BULL"): continue
            # Filtro OB — si hay OB, el precio debe estar cerca de él (máximo 0.5% de distancia)
            # Evita entradas lejos del OB persiguiendo el precio
            if ob:
                if direction == "SHORT":
                    distancia_ob = (ob["low"] - price) / price
                    if distancia_ob > 0.005: continue
                if direction == "LONG":
                    distancia_ob = (price - ob["high"]) / price
                    if distancia_ob > 0.005: continue

            # HH/LL — condición de entrada + score
            # Si hay HH para SHORT o LL para LONG → suma puntos y confirma entrada
            # Si NO hay OB ni FVG ni HH/LL → bloquear señal (sin zona de referencia)
            hh_ll_valido = (direction == "SHORT" and hh_ll_type == "HH") or \
                           (direction == "LONG"  and hh_ll_type == "LL")
            tiene_zona = ob or fvg or hh_ll_valido
            if not tiene_zona: continue  # Sin ninguna zona de referencia, no entrar

            if hh_ll_valido:
                score = min(score + 2, 10)  # +2 puntos — es una zona de alta probabilidad
                if direction == "SHORT":
                    labels.append("HH — zona de oferta " + fmt(hh_ll_level))
                else:
                    labels.append("LL — zona de demanda " + fmt(hh_ll_level))
            if direction == "SHORT" and near_sup: continue
            if direction == "LONG"  and near_res: continue
            # Filtro volumen mínimo — no operar con volumen menor al 20% del promedio
            if vol_r < 20: continue
            if score >= MIN_SCORE:
                # Verificar cooldown — no repetir misma alerta
                if ya_alerte(symbol, direction, interval):
                    continue
                sl, tp1, tp2, tp3, rr1, rr2 = calc_sl_tp(price, direction, sup, res)
                # Si es rebote, usar SL ajustado (1% del precio) y TPs cortos
                if tipo_setup == "REBOTE":
                    risk = price * 0.01  # SL fijo al 1% del precio, más ajustado
                    if direction == "LONG":
                        sl  = round(price - risk, 6)
                        tp1 = round(price + risk * 0.8, 6)
                        tp2 = round(price + risk * 1.5, 6)
                        tp3 = round(price + risk * 2.0, 6)
                    else:
                        sl  = round(price + risk, 6)
                        tp1 = round(price - risk * 0.8, 6)
                        tp2 = round(price - risk * 1.5, 6)
                        tp3 = round(price - risk * 2.0, 6)
                    rr1 = round(abs(tp1 - price) / risk, 1)
                    rr2 = round(abs(tp2 - price) / risk, 1)
                results.append({
                    "symbol":symbol, "direction":direction, "price":price,
                    "rsi":rsi, "score":score, "labels":labels,
                    "support":sup, "resistance":res,
                    "ob":ob, "fvg":fvg, "structure":structure,
                    "candles":candles, "vol_ratio":vol_r,
                    "divergence":divergence, "htf_bias":htf_bias,
                    "sl":sl, "tp1":tp1, "tp2":tp2, "tp3":tp3,
                    "rr1":rr1, "rr2":rr2, "volatility":vol,
                    "tipo_setup":tipo_setup,
                    "hh_ll_type":hh_ll_type, "hh_ll_level":hh_ll_level
                })
        return results if results else None
    except Exception as e:
        print("Error " + symbol + ": " + str(e))
        return None

def scan_all():
    now = datetime.now(ARG_TZ).strftime("%H:%M")
    ts  = datetime.now(ARG_TZ).strftime("%d/%m %H:%M:%S")
    print("[" + now + "] Escaneando...")

    all_setups = []

    for interval, tf_label in INTERVALS:
        for symbol in SYMBOLS:
            try:
                results = analyze_symbol(symbol, interval)
                if results:
                    for r in results:
                        r["tf_label"] = tf_label
                    all_setups.extend(results)
            except Exception as e:
                print("Error " + symbol + " " + interval + ": " + str(e))
            time.sleep(0.15)

    all_setups.sort(key=lambda x: x["score"], reverse=True)

    msg = "🔍 <b>SCANNER v4.2 — " + now + "</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += "📋 Activos: " + str(len(SYMBOLS)) + " x 2 TF | Setups: " + str(len(all_setups)) + "\n"
    msg += "⚙️ Score: " + str(MIN_SCORE) + "/10 | Cooldown: " + str(ALERTA_COOLDOWN_MIN) + "min\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"

    if all_setups:
        longs  = [s for s in all_setups if s["direction"] == "LONG"]
        shorts = [s for s in all_setups if s["direction"] == "SHORT"]
        if longs:
            msg += "\n🟢 <b>SETUPS LONG</b>\n"
            for s in longs:
                guardar_alerta(s, s["tf_label"])
                msg += format_setup(s, s["tf_label"]) + "\n"
        if shorts:
            msg += "\n🔴 <b>SETUPS SHORT</b>\n"
            for s in shorts:
                guardar_alerta(s, s["tf_label"])
                msg += format_setup(s, s["tf_label"]) + "\n"
    else:
        msg += "\n⏳ Sin setups ahora.\n"

    msg += "\n🕐 " + ts
    msg += "\n⚠️ No es consejo financiero."

    send_telegram(msg)
    print("[" + now + "] Alerta enviada - " + str(len(all_setups)) + " setups")

def resumen_diario():
    """Manda resumen diario a las 8am con las alertas del dia anterior"""
    historial = get_historial_alertas(20)
    if not historial:
        return
    msg = "📅 <b>RESUMEN DIARIO - " + datetime.now(ARG_TZ).strftime("%d/%m/%Y") + "</b>\n\n"
    msg += "Ultimas " + str(len(historial)) + " alertas:\n"
    for row in historial:
        symbol, direction, timeframe, precio, score, enviada_at = row
        emoji = "🟢" if direction == "LONG" else "🔴"
        msg += emoji + " " + ("Long" if direction == "LONG" else "Short") + " " + symbol
        msg += " [" + timeframe + "] Score:" + str(score)
        msg += " @ " + fmt(precio)
        msg += " - " + enviada_at.strftime("%d/%m %H:%M") + "\n"
    send_telegram(msg)

if __name__ == "__main__":
    print("Bot Scanner Crypto v4.0 con memoria iniciado...")
    init_db()
    send_telegram(
        "<b>Bot Scanner Crypto v4.2 ACTIVO</b>\n\n"
        "Nuevo en v4.2:\n"
        "- Diferencia REVERSIÓN vs REBOTE\n"
        "- Rebotes: advertencia + TPs más cortos\n"
        "- Reversión: CHoCH + divergencia + HTF confirmados\n"
        "- Score minimo: " + str(MIN_SCORE) + "/10\n"
        "- Cooldown: " + str(ALERTA_COOLDOWN_MIN) + "min\n\n"
        "Escaneando cada 5 minutos..."
    )
    scan_all()
    schedule.every(5).minutes.do(scan_all)
    schedule.every().day.at("08:00").do(resumen_diario)
    while True:
        schedule.run_pending()
        time.sleep(30)
