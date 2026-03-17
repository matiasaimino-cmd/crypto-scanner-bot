import requests
import pandas as pd
import numpy as np
import time
import schedule
from datetime import datetime

TELEGRAM_TOKEN = "8621482285:AAFXlOcgNwRQp1MMmYABaDLUXrXAoQgDplc"
CHAT_ID        = "1343270628"
BINANCE_BASE   = "https://api.binance.com/api/v3"
MIN_SCORE      = 3
RSI_OVERBOUGHT = 70
RSI_OVERSOLD   = 30
RSI_EXTREME    = 75

SYMBOLS = [
    "BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","BNBUSDT",
    "DOGEUSDT","ADAUSDT","AVAXUSDT","DOTUSDT","MATICUSDT",
    "LINKUSDT","LTCUSDT","ATOMUSDT","NEARUSDT","HBARUSDT",
    "THETAUSDT","FTMUSDT","SANDUSDT","MANAUSDT","INJUSDT"
]

INTERVALS = [("15m","Scalping 15m"),("1h","Day Trading 1H"),("4h","Swing 4H")]

def send_telegram(message):
    url = "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/sendMessage"
    for chunk in [message[i:i+4000] for i in range(0, len(message), 4000)]:
        try:
            requests.post(url, json={"chat_id": CHAT_ID, "text": chunk, "parse_mode": "HTML"}, timeout=10)
            time.sleep(0.5)
        except Exception as e:
            print("Error Telegram: " + str(e))

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
        gain  = delta.where(delta > 0, 0.0).rolling(period).mean()
        loss  = (-delta.where(delta < 0, 0.0)).rolling(period).mean()
        rs    = gain / loss
        val   = (100 - 100 / (1 + rs)).iloc[-1]
        return round(val, 2) if not np.isnan(val) else 50.0
    except:
        return 50.0

# ============================================================
# MEJORA 1 — DIVERGENCIA RSI: lookback ampliado a 50 velas
# ============================================================
def detect_rsi_divergence(df, period=14):
    try:
        closes = df["close"]
        delta  = closes.diff()
        gain   = delta.where(delta > 0, 0.0).rolling(period).mean()
        loss   = (-delta.where(delta < 0, 0.0)).rolling(period).mean()
        rsi_full = 100 - 100 / (1 + gain / loss)

        lookback   = 100  # ampliado de 30 a 50
        recent_df  = df.iloc[-lookback:]
        recent_rsi = rsi_full.iloc[-lookback:]

        price_highs, rsi_highs = [], []
        price_lows,  rsi_lows  = [], []

        for i in range(3, len(recent_df) - 3):  # ventana mas amplia
            ph = recent_df["high"].iloc[i]
            pl = recent_df["low"].iloc[i]
            rh = recent_rsi.iloc[i]

            # Swing high mas robusto — 3 velas a cada lado
            if (ph > recent_df["high"].iloc[i-1] and ph > recent_df["high"].iloc[i+1] and
                ph > recent_df["high"].iloc[i-2] and ph > recent_df["high"].iloc[i+2]):
                price_highs.append((i, ph))
                rsi_highs.append((i, rh))

            # Swing low mas robusto
            if (pl < recent_df["low"].iloc[i-1] and pl < recent_df["low"].iloc[i+1] and
                pl < recent_df["low"].iloc[i-2] and pl < recent_df["low"].iloc[i+2]):
                price_lows.append((i, pl))
                rsi_lows.append((i, rh))

        divergence = None

        # Divergencia bajista: precio HH pero RSI LH
        if len(price_highs) >= 2 and len(rsi_highs) >= 2:
            if (price_highs[-1][1] > price_highs[-2][1] and
                rsi_highs[-1][1]   < rsi_highs[-2][1] and
                rsi_highs[-1][1] > 50):  # solo valida si RSI sigue alto
                divergence = "BEARISH_DIV"

        # Divergencia alcista: precio LL pero RSI HL
        if len(price_lows) >= 2 and len(rsi_lows) >= 2:
            if (price_lows[-1][1] < price_lows[-2][1] and
                rsi_lows[-1][1]   > rsi_lows[-2][1] and
                rsi_lows[-1][1] < 50):  # solo valida si RSI sigue bajo
                divergence = "BULLISH_DIV"

        return divergence
    except:
        return None

def get_htf_bias(symbol, interval):
    try:
        next_tf = "4h" if interval in ("15m","1h") else "1d"
        df = get_klines(symbol, next_tf, limit=100)
        if df is None: return "NEUTRAL"
        closes = df["close"]
        price  = closes.iloc[-1]
        ema50  = closes.ewm(span=50,  adjust=False).mean().iloc[-1]
        ema200 = closes.ewm(span=200, adjust=False).mean().iloc[-1]
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

# ============================================================
# MEJORA 2 — ORDER BLOCKS: lookback ampliado + validacion mejorada
# ============================================================
def detect_ob(df):
    ob_bull, ob_bear = None, None
    try:
        price = df["close"].iloc[-1]
        # Lookback ampliado a 200 velas, busca el OB mas reciente y valido
        for i in range(5, min(len(df)-2, 200)):
            c = df.iloc[i]
            body = abs(c["close"] - c["open"])
            rng  = c["high"] - c["low"]
            if rng == 0: continue
            ratio = body / rng

            end = min(i+10, len(df))  # ventana de confirmacion ampliada

            # OB alcista: vela bajista fuerte + precio sube y rompe el high
            if c["close"] < c["open"] and ratio > 0.55:  # umbral bajado de 0.6 a 0.55
                future_high = df["high"].iloc[i+1:end].max()
                if future_high > c["high"]:
                    # Precio dentro o cerca de la zona OB (tolerancia ampliada a 3%)
                    if c["low"] * 0.97 <= price <= c["high"] * 1.03:
                        ob_bull = {"high": round(c["high"],6), "low": round(c["low"],6)}

            # OB bajista: vela alcista fuerte + precio baja y rompe el low
            if c["close"] > c["open"] and ratio > 0.55:
                future_low = df["low"].iloc[i+1:end].min()
                if future_low < c["low"]:
                    if c["low"] * 0.97 <= price <= c["high"] * 1.03:
                        ob_bear = {"high": round(c["high"],6), "low": round(c["low"],6)}

    except:
        pass
    return ob_bull, ob_bear

# ============================================================
# MEJORA 3 — FVG: condicion ampliada + multiples gaps
# ============================================================
def detect_fvg(df):
    fvg_bull, fvg_bear = None, None
    try:
        price = df["close"].iloc[-1]

        # Busca en las ultimas 100 velas (antes 200 completas pero muy lento)
        search_df = df.iloc[-100:]

        for i in range(2, len(search_df)):
            # FVG alcista: low[i] > high[i-2] — gap hacia arriba
            if search_df["low"].iloc[i] > search_df["high"].iloc[i-2]:
                gl = search_df["high"].iloc[i-2]
                gh = search_df["low"].iloc[i]
                gap_size = (gh - gl) / price * 100  # tamanio del gap en %

                # Solo FVGs significativos (>0.05%) y precio cerca (tolerancia 1%)
                if gap_size > 0.05 and gl * 0.99 <= price <= gh * 1.01:
                    fvg_bull = {"high": round(gh,6), "low": round(gl,6), "size": round(gap_size,3)}

            # FVG bajista: high[i] < low[i-2] — gap hacia abajo
            if search_df["high"].iloc[i] < search_df["low"].iloc[i-2]:
                gh = search_df["low"].iloc[i-2]
                gl = search_df["high"].iloc[i]
                gap_size = (gh - gl) / price * 100

                if gap_size > 0.05 and gl * 0.99 <= price <= gh * 1.01:
                    fvg_bear = {"high": round(gh,6), "low": round(gl,6), "size": round(gap_size,3)}

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
        if lc > lh and pc <= lh: return "BOS_BULL" if lh > ph else "CHoCH_BULL"
        if lc < ll and pc >= ll: return "BOS_BEAR" if ll < pl else "CHoCH_BEAR"
    except:
        pass
    return None

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
        return ratio, ratio >= 120
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

def calc_score(direction, rsi, ob, fvg, structure, patterns, vol_high, near_sr, divergence, htf_bias):
    score, labels = 0, []

    rsi_extreme_short = direction == "SHORT" and rsi >= RSI_EXTREME
    rsi_extreme_long  = direction == "LONG"  and rsi <= (100 - RSI_EXTREME)

    if direction == "LONG":
        if rsi <= RSI_OVERSOLD:   score += 2; labels.append("RSI sobrevendido (" + str(rsi) + ")")
        if ob:    score += 2; labels.append("OB alcista " + fmt(ob["low"]) + "-" + fmt(ob["high"]))
        if fvg:   score += 1.5; labels.append("FVG alcista " + fmt(fvg["low"]) + "-" + fmt(fvg["high"]) + " (" + str(fvg.get("size","?")) + "%)")
        if structure in ("BOS_BULL","CHoCH_BULL"): score += 1.5; labels.append("Estructura: " + structure)
        bull_c = [p for p in patterns if "alcista" in p or "Doji" in p]
        if bull_c: score += 1; labels.append(" | ".join(bull_c))
        if divergence == "BULLISH_DIV": score += 2; labels.append("Divergencia RSI alcista")
        if htf_bias == "BULLISH":
            score += 2; labels.append("HTF a favor (ALCISTA)")
        elif htf_bias == "BEARISH" and not rsi_extreme_long:
            score -= 1; labels.append("HTF en contra (BAJISTA)")
    else:
        if rsi >= RSI_OVERBOUGHT: score += 2; labels.append("RSI sobrecomprado (" + str(rsi) + ")")
        if ob:    score += 2; labels.append("OB bajista " + fmt(ob["low"]) + "-" + fmt(ob["high"]))
        if fvg:   score += 1.5; labels.append("FVG bajista " + fmt(fvg["low"]) + "-" + fmt(fvg["high"]) + " (" + str(fvg.get("size","?")) + "%)")
        if structure in ("BOS_BEAR","CHoCH_BEAR"): score += 1.5; labels.append("Estructura: " + structure)
        bear_c = [p for p in patterns if "bajista" in p or "Doji" in p]
        if bear_c: score += 1; labels.append(" | ".join(bear_c))
        if divergence == "BEARISH_DIV": score += 2; labels.append("Divergencia RSI bajista")
        if htf_bias == "BEARISH":
            score += 2; labels.append("HTF a favor (BAJISTA)")
        elif htf_bias == "BULLISH" and not rsi_extreme_short:
            score -= 1; labels.append("HTF en contra (ALCISTA)")

    if vol_high:  score += 1; labels.append("Volumen elevado")
    if near_sr:   score += 0.5; labels.append("Precio en S/R clave")

    if rsi_extreme_short or rsi_extreme_long:
        score = max(score, MIN_SCORE)
        if not any("extremo" in l for l in labels):
            labels.insert(0, "RSI extremo (" + str(rsi) + ") — alerta directa")

    return round(min(score, 10)), labels

def format_setup(s, tf_label):
    d = s["direction"]
    htf_label = "ALCISTA" if s["htf_bias"] == "BULLISH" else ("BAJISTA" if s["htf_bias"] == "BEARISH" else "NEUTRAL")
    lines = [
        "",
        ("Long" if d == "LONG" else "Short") + " - <b>" + s["symbol"] + "</b> [" + tf_label + "] Score: <b>" + str(s["score"]) + "/10</b>",
        "Precio:      " + fmt(s["price"]),
        "RSI:         " + str(s["rsi"]),
        "HTF Bias:    " + htf_label,
        "Volatilidad: " + str(s["volatility"]) + "%",
        "Volumen:     " + str(s["vol_ratio"]) + "% del promedio",
    ]
    if s["divergence"]:
        lines.append("Divergencia: " + ("Alcista" if s["divergence"] == "BULLISH_DIV" else "Bajista"))
    if s["ob"]:
        lines.append("Order Block: " + fmt(s["ob"]["low"]) + " - " + fmt(s["ob"]["high"]))
    if s["fvg"]:
        sz = s["fvg"].get("size", "")
        lines.append("FVG:         " + fmt(s["fvg"]["low"]) + " - " + fmt(s["fvg"]["high"]) + (" (" + str(sz) + "%)" if sz else ""))
    if s["structure"]:
        lines.append("Estructura:  " + s["structure"])
    lines.append("Vela:        " + (" | ".join(s["candles"]) if s["candles"] else "Sin patron"))
    lines += [
        "Soporte:     " + fmt(s["support"]),
        "Resistencia: " + fmt(s["resistance"]),
        "---",
        "SL:   " + fmt(s["sl"]),
        "TP1:  " + fmt(s["tp1"]) + " (R:R 1:" + str(s["rr1"]) + ")",
        "TP2:  " + fmt(s["tp2"]) + " (R:R 1:" + str(s["rr2"]) + ")",
        "TP3:  " + fmt(s["tp3"]),
        "Confluencias:",
    ]
    for l in s["labels"]:
        lines.append("  - " + l)
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
        near_sup     = abs(price - sup) / price < 0.005
        near_res     = abs(price - res) / price < 0.005
        results = []
        for direction, ob, fvg, near in [("LONG",ob_b,fv_b,near_sup),("SHORT",ob_s,fv_s,near_res)]:
            score, labels = calc_score(direction, rsi, ob, fvg, structure, candles, vol_h, near, divergence, htf_bias)
            if score >= MIN_SCORE:
                sl, tp1, tp2, tp3, rr1, rr2 = calc_sl_tp(price, direction, sup, res)
                results.append({
                    "symbol":symbol, "direction":direction, "price":price,
                    "rsi":rsi, "score":score, "labels":labels,
                    "support":sup, "resistance":res,
                    "ob":ob, "fvg":fvg, "structure":structure,
                    "candles":candles, "vol_ratio":vol_r,
                    "divergence":divergence, "htf_bias":htf_bias,
                    "sl":sl, "tp1":tp1, "tp2":tp2, "tp3":tp3,
                    "rr1":rr1, "rr2":rr2, "volatility":vol
                })
        return results if results else None
    except Exception as e:
        print("Error " + symbol + ": " + str(e))
        return None

def scan_all():
    now = datetime.now().strftime("%H:%M")
    ts  = datetime.now().strftime("%d/%m %H:%M:%S")
    print("[" + now + "] Escaneando 3 timeframes...")

    all_setups, volatilities = [], []

    for interval, tf_label in INTERVALS:
        for symbol in SYMBOLS:
            try:
                results = analyze_symbol(symbol, interval)
                if results:
                    for r in results:
                        r["tf_label"] = tf_label
                    all_setups.extend(results)
                if interval == "15m":
                    df = get_klines(symbol, interval, 50)
                    if df is not None and len(df) > 5:
                        volatilities.append({
                            "symbol": symbol,
                            "volatility": calc_volatility(df),
                            "price": df["close"].iloc[-1]
                        })
            except Exception as e:
                print("Error " + symbol + " " + interval + ": " + str(e))
            time.sleep(0.15)

    all_setups.sort(key=lambda x: x["score"], reverse=True)
    vol_sorted = sorted(volatilities, key=lambda x: x["volatility"], reverse=True)

    msg = "<b>SCANNER v3.1 - " + now + "</b>\n"
    msg += "Activos: " + str(len(SYMBOLS)) + " x 3 TF | Setups: " + str(len(all_setups)) + "\n"
    msg += "RSI extremo (" + str(RSI_EXTREME) + "+) = alerta directa\n"
    msg += "---\n"

    if all_setups:
        longs  = [s for s in all_setups if s["direction"] == "LONG"]
        shorts = [s for s in all_setups if s["direction"] == "SHORT"]
        if longs:
            msg += "\nSETUPS LONG:\n"
            for s in longs: msg += format_setup(s, s["tf_label"]) + "\n"
        if shorts:
            msg += "\nSETUPS SHORT:\n"
            for s in shorts: msg += format_setup(s, s["tf_label"]) + "\n"
    else:
        msg += "Sin setups ahora. Score minimo: " + str(MIN_SCORE) + "/10\n"

    msg += "\nRANKING VOLATILIDAD:\n"
    for i, v in enumerate(vol_sorted, 1):
        msg += str(i) + ". " + v["symbol"] + " - " + str(v["volatility"]) + "% | " + fmt(v["price"]) + "\n"

    msg += "\nActualizado: " + ts
    msg += "\nNo es consejo financiero."

    send_telegram(msg)
    print("[" + now + "] Alerta enviada - " + str(len(all_setups)) + " setups")

if __name__ == "__main__":
    print("Bot Scanner Crypto v3.1 iniciado...")
    send_telegram(
        "<b>Bot Scanner Crypto v3.1 ACTIVO</b>\n\n"
        "Mejoras v3.1:\n"
        "- OB: lookback 200 velas + tolerancia 3%\n"
        "- FVG: filtro por tamanio + tolerancia 1%\n"
        "- Divergencia RSI: lookback 50 velas + validacion\n"
        "- 3 timeframes: 15m / 1H / 4H\n"
        "- RSI extremo (" + str(RSI_EXTREME) + "+) = alerta directa\n"
        "- Score minimo: " + str(MIN_SCORE) + "/10\n\n"
        "Escaneando cada 15 minutos..."
    )
    scan_all()
    schedule.every(5).minutes.do(scan_all)
    while True:
        schedule.run_pending()
        time.sleep(30)
