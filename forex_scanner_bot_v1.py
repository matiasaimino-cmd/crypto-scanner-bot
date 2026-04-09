# =============================================================================
#   FOREX & COMMODITIES SCANNER BOT v1.0
#   Autor: matiasaimino-cmd
#   Descripción: Scanner técnico para Forex y Materias Primas
#   Datos: yFinance (gratuito, delay ~15min — OK para 1H/4H)
#   Plataforma: Railway + PostgreSQL + Telegram
# =============================================================================

import yfinance as yf
import pandas as pd
import numpy as np
import time
import schedule
import psycopg2
import os
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =============================================================================
#   CONFIGURACIÓN GENERAL
# =============================================================================

ARG_TZ              = ZoneInfo("America/Argentina/Buenos_Aires")
TELEGRAM_TOKEN      = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID             = os.environ.get("TELEGRAM_CHAT_ID", "")

if not TELEGRAM_TOKEN or not CHAT_ID:
    raise SystemExit("❌ Faltan TELEGRAM_TOKEN o TELEGRAM_CHAT_ID en variables de entorno")

MIN_SCORE           = 7
RSI_OVERBOUGHT      = 70
RSI_OVERSOLD        = 30
RSI_EXTREME         = 75
ALERTA_COOLDOWN_MIN = 60   # 1 hora — en Forex las señales duran más

# Session HTTP con reintentos
_session = requests.Session()
_retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
_session.mount("https://", HTTPAdapter(max_retries=_retries))

# =============================================================================
#   SÍMBOLOS — Yahoo Finance format
# =============================================================================

# Forex majors + Commodities
SYMBOLS = {
    # Forex
    "EURUSD": "EURUSD=X",
    "GBPUSD": "GBPUSD=X",
    "USDJPY": "USDJPY=X",
    "AUDUSD": "AUDUSD=X",
    "USDCHF": "USDCHF=X",
    "NZDUSD": "NZDUSD=X",
    "USDCAD": "USDCAD=X",
    "EURGBP": "EURGBP=X",
    # Commodities
    "XAUUSD": "GC=F",     # Oro
    "XAGUSD": "SI=F",     # Plata
    "USOIL":  "CL=F",     # Petróleo WTI
    "UKOIL":  "BZ=F",     # Petróleo Brent
}

# Timeframes — yfinance format
INTERVALS = [
    ("1h",  "Day Trading 1H",   "1h",  "5d"),    # (interno, label, yf_interval, yf_period)
    ("4h",  "Swing Trading 4H", "1h",  "60d"),   # 4H simulado con 1H y agrupado
]

# Sesiones de trading (hora Argentina UTC-3)
# Londres: 06:00-15:00 ART | Nueva York: 11:00-20:00 ART
SESION_INICIO = 6    # 6am ART = Londres abre
SESION_FIN    = 20   # 8pm ART = NY cierra

# =============================================================================
#   BASE DE DATOS
# =============================================================================

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
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fx_alertas (
                id           SERIAL PRIMARY KEY,
                symbol       VARCHAR(20),
                direction    VARCHAR(10),
                timeframe    VARCHAR(20),
                precio       FLOAT,
                rsi          FLOAT,
                score        FLOAT,
                sl           FLOAT,
                tp1          FLOAT,
                tp2          FLOAT,
                tp3          FLOAT,
                confluencias TEXT,
                enviada_at   TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fx_market_state (
                id             SERIAL PRIMARY KEY,
                symbol         VARCHAR(20),
                timeframe      VARCHAR(20),
                rsi            FLOAT,
                estructura     TEXT,
                htf_bias       TEXT,
                divergencia    TEXT,
                precio         FLOAT,
                actualizado_at TIMESTAMPTZ DEFAULT NOW(),
                CONSTRAINT fx_market_state_unique UNIQUE (symbol, timeframe)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fx_tracking (
                id            SERIAL PRIMARY KEY,
                alerta_id     INTEGER,
                symbol        VARCHAR(20),
                direction     VARCHAR(10),
                timeframe     VARCHAR(20),
                precio_entry  FLOAT,
                sl            FLOAT,
                tp1           FLOAT,
                tp2           FLOAT,
                tp3           FLOAT,
                score         FLOAT,
                condicion     VARCHAR(20) DEFAULT 'TENDENCIA',
                resultado     VARCHAR(10) DEFAULT 'OPEN',
                tp_alcanzado  INTEGER DEFAULT 0,
                precio_cierre FLOAT,
                pnl_pct       FLOAT,
                abierto_at    TIMESTAMPTZ DEFAULT NOW(),
                cerrado_at    TIMESTAMPTZ
            )
        """)
        try:
            cur.execute("""
                DELETE FROM fx_market_state
                WHERE id NOT IN (
                    SELECT MAX(id) FROM fx_market_state
                    GROUP BY symbol, timeframe
                )
            """)
            conn.commit()
        except:
            conn.rollback()

        print("DB Forex iniciada OK")
    except Exception as e:
        print("Error init DB Forex: " + str(e))
    finally:
        conn.close()


def ya_alerte_fx(symbol, direction, timeframe):
    conn = get_db()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM fx_alertas
            WHERE symbol    = %s
              AND direction = %s
              AND timeframe = %s
              AND enviada_at > NOW() - (INTERVAL '1 minute' * %s)
        """, (symbol, direction, timeframe, ALERTA_COOLDOWN_MIN))
        return cur.fetchone()[0] > 0
    except:
        return False
    finally:
        conn.close()


def guardar_alerta_fx(s, tf_label):
    conn = get_db()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO fx_alertas
                (symbol, direction, timeframe, precio, rsi, score, sl, tp1, tp2, tp3, confluencias)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            s["symbol"], s["direction"], tf_label,
            float(s["price"]), float(s["rsi"]), float(s["score"]),
            float(s["sl"]),    float(s["tp1"]), float(s["tp2"]),  float(s["tp3"]),
            " | ".join(s["labels"])
        ))
        alerta_id = cur.fetchone()[0]
        conn.commit()

        # Crear tracking solo si no hay uno OPEN del mismo par/dirección/timeframe
        cur.execute("""
            SELECT COUNT(*) FROM fx_tracking
            WHERE symbol    = %s
              AND direction = %s
              AND timeframe = %s
              AND resultado = 'OPEN'
        """, (s["symbol"], s["direction"], tf_label))
        if cur.fetchone()[0] == 0:
            cur.execute("""
                INSERT INTO fx_tracking
                    (alerta_id, symbol, direction, timeframe, precio_entry, sl, tp1, tp2, tp3, score, condicion)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                alerta_id, s["symbol"], s["direction"], tf_label,
                float(s["price"]), float(s["sl"]),
                float(s["tp1"]),   float(s["tp2"]), float(s["tp3"]),
                float(s["score"]), s.get("condicion_mercado", "TENDENCIA")
            ))
        conn.commit()
    except Exception as e:
        print("Error guardar alerta FX: " + str(e))
    finally:
        conn.close()


def verificar_resultados_fx():
    """Verifica resultados de operaciones Forex abiertas, aplica trailing stop y notifica"""
    conn = get_db()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, symbol, direction, precio_entry, sl, tp1, tp2, tp3, abierto_at, timeframe
            FROM fx_tracking WHERE resultado = 'OPEN'
        """)
        operaciones = cur.fetchall()
        if not operaciones: return

        for op in operaciones:
            op_id, symbol, direction, entry, sl, tp1, tp2, tp3, abierto_at, tf = op
            try:
                symbol_yf = SYMBOLS.get(symbol)
                if not symbol_yf: continue
                df = get_klines_fx(symbol_yf, "1h", limit=30)
                if df is None: continue
                precio_actual = float(df["close"].iloc[-1])

                # Trailing stop
                try:
                    atr_pct_trail = calc_atr(df)
                    nuevo_sl      = calcular_trailing_sl(direction, precio_actual, entry, sl, atr_pct_trail)
                    if nuevo_sl != sl:
                        cur.execute("UPDATE fx_tracking SET sl = %s WHERE id = %s", (nuevo_sl, op_id))
                        conn.commit()
                        sl = nuevo_sl
                except Exception as e:
                    print("Error trailing FX " + symbol + ": " + str(e))

                resultado = None; tp_alcanzado = 0; pnl_pct = 0.0

                if direction == "LONG":
                    if precio_actual <= sl:
                        resultado = "LOSS"; pnl_pct = round((precio_actual - entry) / entry * 100, 2)
                    elif precio_actual >= tp3:
                        resultado = "WIN"; tp_alcanzado = 3; pnl_pct = round((precio_actual - entry) / entry * 100, 2)
                    elif precio_actual >= tp2:
                        resultado = "WIN"; tp_alcanzado = 2; pnl_pct = round((precio_actual - entry) / entry * 100, 2)
                    elif precio_actual >= tp1:
                        resultado = "WIN"; tp_alcanzado = 1; pnl_pct = round((precio_actual - entry) / entry * 100, 2)
                else:
                    if precio_actual >= sl:
                        resultado = "LOSS"; pnl_pct = round((entry - precio_actual) / entry * 100, 2)
                    elif precio_actual <= tp3:
                        resultado = "WIN"; tp_alcanzado = 3; pnl_pct = round((entry - precio_actual) / entry * 100, 2)
                    elif precio_actual <= tp2:
                        resultado = "WIN"; tp_alcanzado = 2; pnl_pct = round((entry - precio_actual) / entry * 100, 2)
                    elif precio_actual <= tp1:
                        resultado = "WIN"; tp_alcanzado = 1; pnl_pct = round((entry - precio_actual) / entry * 100, 2)

                # Expirar según timeframe
                if resultado is None:
                    now_tz     = datetime.now(ARG_TZ)
                    abierto_tz = abierto_at if abierto_at.tzinfo else abierto_at.replace(tzinfo=ARG_TZ)
                    horas      = (now_tz - abierto_tz).total_seconds() / 3600
                    max_horas  = 120 if "4H" in tf else 24
                    if horas > max_horas:
                        resultado = "EXPIRED"
                        pnl_pct   = round((entry - precio_actual) / entry * 100 * (-1 if direction == "LONG" else 1), 2)

                if resultado:
                    cur.execute("""
                        UPDATE fx_tracking SET resultado=%s, tp_alcanzado=%s,
                        precio_cierre=%s, pnl_pct=%s, cerrado_at=NOW() WHERE id=%s
                    """, (resultado, tp_alcanzado, precio_actual, pnl_pct, op_id))
                    conn.commit()
                    if resultado in ("WIN", "LOSS"):
                        emoji  = "✅" if resultado == "WIN" else "❌"
                        emoji2 = "🟢" if direction == "LONG" else "🔴"
                        msg    = emoji + " <b>" + resultado + "</b> — " + emoji2 + " " + direction + " " + symbol + "\n"
                        if resultado == "WIN": msg += "🎯 TP" + str(tp_alcanzado) + " alcanzado\n"
                        else:                  msg += "🛑 SL tocado\n"
                        msg += "📊 Entrada: " + fmt_fx(entry) + " → " + fmt_fx(precio_actual) + "\n"
                        msg += "💰 P&L: <b>" + ("+" if pnl_pct > 0 else "") + str(pnl_pct) + "%</b>"
                        send_telegram(msg)
            except Exception as e:
                print("Error verificar FX " + symbol + ": " + str(e))
    except Exception as e:
        print("Error verificar_resultados_fx: " + str(e))
    finally:
        conn.close()


def reporte_tracking_fx():
    """Reporte de rendimiento del bot Forex — comando /fxreporte"""
    conn = get_db()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT resultado, COUNT(*), AVG(pnl_pct), MAX(pnl_pct), MIN(pnl_pct)
            FROM fx_tracking WHERE resultado != 'OPEN' GROUP BY resultado
        """)
        stats  = cur.fetchall()
        cur.execute("""
            SELECT symbol, direction, timeframe, precio_entry, pnl_pct, resultado, tp_alcanzado, abierto_at
            FROM fx_tracking WHERE resultado != 'OPEN' ORDER BY abierto_at DESC LIMIT 20
        """)
        ultimas = cur.fetchall()

        if not stats:
            send_telegram("📊 Sin operaciones Forex cerradas todavía.")
            return

        wins   = next((r for r in stats if r[0] == "WIN"),  (None, 0, 0, 0, 0))
        losses = next((r for r in stats if r[0] == "LOSS"), (None, 0, 0, 0, 0))
        total  = sum(r[1] for r in stats if r[0] in ("WIN", "LOSS"))
        wr     = round(wins[1] / total * 100, 1) if total > 0 else 0

        msg  = "📊 <b>REPORTE FOREX</b>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━\n"
        msg += "Total: <b>" + str(total) + "</b> operaciones\n"
        msg += "✅ Ganadoras: <b>" + str(wins[1]) + "</b> (" + str(wr) + "%)\n"
        msg += "❌ Perdedoras: <b>" + str(losses[1]) + "</b>\n"
        if wins[2]:   msg += "💰 P&L prom WIN:  <b>+" + str(round(wins[2], 2)) + "%</b>\n"
        if losses[2]: msg += "💸 P&L prom LOSS: <b>" + str(round(losses[2], 2)) + "%</b>\n"
        msg += "\n📋 <b>Últimas 20:</b>\n"
        for row in ultimas:
            symbol, direction, tf, entry, pnl, resultado, tp, fecha = row
            if resultado == "WIN":   emoji = "✅ TP" + str(tp)
            elif resultado == "LOSS": emoji = "❌ SL"
            else:                    emoji = "⏰ EXP"
            dir_emoji = "🟢" if direction == "LONG" else "🔴"
            pnl_str = ("+" if pnl and pnl > 0 else "") + str(round(pnl, 2)) + "%" if pnl else "?"
            msg += dir_emoji + " " + symbol + " " + emoji + " " + pnl_str + " — " + fecha.strftime("%d/%m %H:%M") + "\n"
        send_telegram(msg)
    except Exception as e:
        print("Error reporte_tracking_fx: " + str(e))
    finally:
        conn.close()


def get_historial_fx(limite=20):
    conn = get_db()
    if not conn: return []
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT symbol, direction, timeframe, precio, score, enviada_at
            FROM fx_alertas
            ORDER BY enviada_at DESC
            LIMIT %s
        """, (limite,))
        return cur.fetchall()
    except:
        return []
    finally:
        conn.close()

# =============================================================================
#   TELEGRAM
# =============================================================================

def send_telegram(message):
    """Envía mensaje a Telegram con reintentos y backoff dinámico para 429"""
    url = "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/sendMessage"
    for chunk in [message[i:i+4000] for i in range(0, len(message), 4000)]:
        for intento in range(4):
            try:
                r = _session.post(
                    url,
                    json={"chat_id": CHAT_ID, "text": chunk, "parse_mode": "HTML"},
                    timeout=10
                )
                if r.status_code == 429:
                    retry_after = r.json().get("parameters", {}).get("retry_after", 5)
                    print("Telegram 429 — esperando " + str(retry_after) + "s")
                    time.sleep(retry_after)
                    continue
                break
            except Exception as e:
                print("Error Telegram intento " + str(intento+1) + ": " + str(e))
                time.sleep(2 ** intento)
        time.sleep(0.5)


def get_telegram_updates(offset=None):
    try:
        url    = "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/getUpdates"
        params = {"timeout": 5}
        if offset: params["offset"] = offset
        r = _session.get(url, params=params, timeout=10)
        return r.json().get("result", [])
    except:
        return []


def procesar_comandos(last_update_id):
    updates = get_telegram_updates(offset=last_update_id)
    for update in updates:
        last_update_id = update["update_id"] + 1
        msg  = update.get("message", {})
        text = msg.get("text", "").strip().lower()
        chat = str(msg.get("chat", {}).get("id", ""))
        if chat != CHAT_ID: continue
        if text == "/fxresumen":
            resumen_diario_fx()
        elif text == "/fxscan":
            send_telegram("🔄 Iniciando escaneo Forex...")
            scan_all_fx()
        elif text == "/fxreporte":
            reporte_tracking_fx()
        elif text == "/fxayuda":
            send_telegram(
                "💱 <b>Forex Scanner — Comandos:</b>\n\n"
                "/fxresumen  — últimas 20 alertas\n"
                "/fxscan     — escaneo inmediato\n"
                "/fxreporte  — rendimiento de operaciones\n"
                "/fxayuda    — esta ayuda"
            )
    return last_update_id

# =============================================================================
#   DATOS — yFINANCE
# =============================================================================

def get_klines_fx(symbol_yf, interval, limit=200):
    """
    Obtiene velas de yFinance y retorna DataFrame.
    Normaliza columnas para que sea compatible con la lógica del bot.
    """
    try:
        # Determinar período según intervalo
        if interval == "1h":
            period = "30d"
        elif interval == "4h":
            period = "60d"
        elif interval == "1d":
            period = "200d"
        else:
            period = "30d"

        ticker = yf.Ticker(symbol_yf)
        df = ticker.history(period=period, interval=interval, auto_adjust=True)

        if df is None or len(df) < 30:
            return None

        # Normalizar columnas
        df = df.rename(columns={
            "Open":   "open",
            "High":   "high",
            "Low":    "low",
            "Close":  "close",
            "Volume": "volume"
        })
        df = df[["open", "high", "low", "close", "volume"]].copy()
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna().reset_index(drop=True)

        # Retornar las últimas `limit` velas
        return df.iloc[-limit:].reset_index(drop=True) if len(df) >= 30 else None
    except Exception as e:
        print("Error get_klines_fx: " + str(e))
        return None


def get_htf_bias_fx(symbol_yf):
    """
    Bias del timeframe diario (1D) para Forex.
    Usa EMA50, EMA200 y estructura de mercado.
    Si no hay suficientes velas para EMA200, usa solo EMA50 y estructura.
    """
    try:
        df = get_klines_fx(symbol_yf, "1d", limit=250)
        if df is None or len(df) < 60: return "NEUTRAL"

        closes = df["close"]
        price  = closes.iloc[-1]
        ema50  = closes.ewm(span=50,  adjust=False, min_periods=50).mean().iloc[-1]
        ema200 = closes.ewm(span=200, adjust=False, min_periods=200).mean().iloc[-1]

        if np.isnan(ema200):
            bull, bear = 0, 0
            if not np.isnan(ema50):
                if price > ema50: bull += 2
                else:             bear += 2
        else:
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


def fmt_fx(p):
    """Formatea precio según magnitud — Forex tiene muchos decimales"""
    try:
        if p >= 100:  return "{:,.2f}".format(p)
        elif p >= 10: return "{:.3f}".format(p)
        elif p >= 1:  return "{:.4f}".format(p)
        else:         return "{:.5f}".format(p)
    except:
        return str(p)


def en_sesion_activa():
    """
    Verifica si el mercado Forex/Commodities está abierto globalmente.
    Sábado cerrado, domingo desde 19:00, viernes hasta 17:00, parate 17-19 L-J.
    """
    now  = datetime.now(ARG_TZ)
    hora = now.hour
    dia  = now.weekday()  # 0=Lunes ... 5=Sábado, 6=Domingo

    if dia == 5: return False           # Sábado cerrado
    if dia == 6: return hora >= 19      # Domingo desde 19:00
    if dia == 4: return hora < 17       # Viernes hasta 17:00
    return not (17 <= hora < 19)        # L-J: parate 17-19


def en_sesion_par(symbol):
    """
    Filtro de sesión específico por par.
    Solo operar cada par en su sesión de mayor liquidez.

    Sesiones (hora Argentina ART = UTC-3):
    - Tokio:   00:00 - 09:00 ART  → USD/JPY, AUD/USD, NZD/USD
    - Londres: 06:00 - 15:00 ART  → EUR/USD, GBP/USD, EUR/GBP, USD/CHF
    - NY:      11:00 - 20:00 ART  → USD/CAD, USD/CHF, todos los majors
    - Overlap: 11:00 - 15:00 ART  → mejor momento para todos

    Commodities: válidos en cualquier sesión activa (siguen a NY principalmente)
    """
    hora = datetime.now(ARG_TZ).hour

    # Sesión Londres + NY overlap (mejor para majors europeos)
    sesion_londres = 6 <= hora < 15
    sesion_ny      = 11 <= hora < 20
    sesion_tokio   = 0 <= hora < 9

    # Pares europeos — solo Londres o NY
    if symbol in ("EURUSD", "GBPUSD", "EURGBP", "USDCHF"):
        return sesion_londres or sesion_ny

    # Pares asiáticos — Tokio o NY
    if symbol in ("USDJPY", "AUDUSD", "NZDUSD"):
        return sesion_tokio or sesion_ny

    # USD/CAD — principalmente NY
    if symbol == "USDCAD":
        return sesion_ny

    # Commodities — siguen a NY pero válidos en Londres también
    if symbol in ("XAUUSD", "XAGUSD", "USOIL", "UKOIL"):
        return sesion_londres or sesion_ny

    # Default — cualquier sesión activa
    return True



# =============================================================================
#   INDICADORES TÉCNICOS (misma lógica que el bot crypto)
# =============================================================================

def calc_rsi(closes, period=14):
    try:
        delta    = closes.diff()
        gain     = delta.where(delta > 0, 0.0)
        loss     = (-delta.where(delta < 0, 0.0))
        avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        rs       = avg_gain / avg_loss
        val      = (100 - 100 / (1 + rs)).iloc[-1]
        return round(val, 2) if not np.isnan(val) else 50.0
    except:
        return 50.0


def detect_rsi_divergence(df, period=14):
    try:
        closes   = df["close"]
        delta    = closes.diff()
        gain     = delta.where(delta > 0, 0.0)
        loss     = (-delta.where(delta < 0, 0.0))
        avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        rsi_full = 100 - 100 / (1 + avg_gain / avg_loss)

        # Lookback dinámico
        lookback   = min(max(len(df) // 2, 50), 100)
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
        if (len(price_highs) >= 2 and price_highs[-1][1] > price_highs[-2][1] and
            rsi_highs[-1][1] < rsi_highs[-2][1] and rsi_highs[-1][1] > 45):
            divergence = "BEARISH_DIV"
        if (len(price_lows) >= 2 and price_lows[-1][1] < price_lows[-2][1] and
            rsi_lows[-1][1] > rsi_lows[-2][1] and rsi_lows[-1][1] < 55):
            divergence = "BULLISH_DIV"
        return divergence
    except:
        return None


def calc_sr(df):
    try:
        price  = df["close"].iloc[-1]
        recent = df.iloc[-100:]
        ph, pl = [], []
        for i in range(5, len(recent) - 5):
            w_h = recent["high"].iloc[i-5:i+6]
            w_l = recent["low"].iloc[i-5:i+6]
            if len(w_h) == 11 and recent["high"].iloc[i] == w_h.max():
                ph.append(recent["high"].iloc[i])
            if len(w_l) == 11 and recent["low"].iloc[i] == w_l.min():
                pl.append(recent["low"].iloc[i])
        pl.append(float(df.iloc[-20:]["low"].min()))
        ph.append(float(df.iloc[-20:]["high"].max()))
        pl.append(float(df.iloc[-50:]["low"].min()))
        ph.append(float(df.iloc[-50:]["high"].max()))
        sups = sorted([l for l in pl if l < price], reverse=True)
        ress = sorted([h for h in ph if h > price])

        # Clustering dinámico según volatilidad del activo
        vol_pct = ((df["high"] - df["low"]) / df["close"] * 100).iloc[-20:].mean()
        cluster_threshold = max(vol_pct * 0.3, 0.1) / 100  # mínimo 0.1%

        def cluster(levels):
            if not levels: return levels
            clustered = [levels[0]]
            for l in levels[1:]:
                if abs(l - clustered[-1]) / price > cluster_threshold:
                    clustered.append(l)
            return clustered

        sups = cluster(sups)
        ress = cluster(ress)
        return (round(sups[0], 6) if sups else round(price * 0.998, 6),
                round(ress[0], 6) if ress else round(price * 1.002, 6))
    except:
        price = df["close"].iloc[-1]
        return round(price * 0.998, 6), round(price * 1.002, 6)


def detect_ob(df):
    """
    Order Blocks. Solo considera los últimos 100 velas para asegurar vigencia.
    """
    ob_bull, ob_bear = None, None
    try:
        price    = df["close"].iloc[-1]
        max_look = min(len(df)-3, 100)
        for i in range(max_look, 4, -1):
            c     = df.iloc[i]
            body  = abs(c["close"] - c["open"])
            rng   = c["high"] - c["low"]
            if rng == 0: continue
            ratio = body / rng
            end   = min(i+10, len(df))
            if ob_bull is None and c["close"] < c["open"] and ratio > 0.55:
                fh = df["high"].iloc[i+1:end].max()
                if fh > c["high"] and c["low"] <= price <= c["high"] * 1.005:
                    ob_bull = {"high": round(c["high"], 6), "low": round(c["low"], 6)}
            if ob_bear is None and c["close"] > c["open"] and ratio > 0.55:
                fl = df["low"].iloc[i+1:end].min()
                if fl < c["low"] and c["low"] * 0.995 <= price <= c["high"]:
                    ob_bear = {"high": round(c["high"], 6), "low": round(c["low"], 6)}
            if ob_bull and ob_bear: break
    except:
        pass
    return ob_bull, ob_bear


def detect_fvg(df):
    fvg_bull, fvg_bear = None, None
    try:
        price     = df["close"].iloc[-1]
        search_df = df.iloc[-100:]
        for i in range(len(search_df)-1, 1, -1):
            if fvg_bull is None and search_df["low"].iloc[i] > search_df["high"].iloc[i-2]:
                gl, gh   = search_df["high"].iloc[i-2], search_df["low"].iloc[i]
                gap_size = (gh - gl) / price * 100
                if gap_size > 0.02 and gl <= price <= gh:
                    fvg_bull = {"high": round(gh, 6), "low": round(gl, 6), "size": round(gap_size, 3)}
            if fvg_bear is None and search_df["high"].iloc[i] < search_df["low"].iloc[i-2]:
                gh, gl   = search_df["low"].iloc[i-2], search_df["high"].iloc[i]
                gap_size = (gh - gl) / price * 100
                if gap_size > 0.02 and gl <= price <= gh:
                    fvg_bear = {"high": round(gh, 6), "low": round(gl, 6), "size": round(gap_size, 3)}
            if fvg_bull and fvg_bear: break
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
        lh, ph        = highs[-1][1], highs[-2][1]
        ll, pl        = lows[-1][1],  lows[-2][1]
        last_high_idx = highs[-1][0]
        last_low_idx  = lows[-1][0]
        for lookback in range(1, 6):
            if lookback >= len(df): break
            lc = df["close"].iloc[-lookback]
            pc = df["close"].iloc[-lookback-1] if lookback+1 < len(df) else lc
            if lc > lh and pc <= lh and (len(df) - last_high_idx) <= 50:
                return "BOS_BULL" if lh > ph else "CHoCH_BULL"
            if lc < ll and pc >= ll and (len(df) - last_low_idx) <= 50:
                return "BOS_BEAR" if ll < pl else "CHoCH_BEAR"
    except:
        pass
    return None


def detect_fibonacci(df, lookback=100):
    try:
        recent = df.iloc[-lookback:]
        price  = recent["close"].iloc[-1]
        swing_high_idx, swing_high_val = None, 0.0
        swing_low_idx,  swing_low_val  = None, float("inf")
        for i in range(3, len(recent) - 3):
            h = recent["high"].iloc[i]
            l = recent["low"].iloc[i]
            if (h > recent["high"].iloc[i-1] and h > recent["high"].iloc[i+1] and
                h > recent["high"].iloc[i-2] and h > recent["high"].iloc[i+2]):
                if h > swing_high_val:
                    swing_high_val = h; swing_high_idx = i
            if (l < recent["low"].iloc[i-1] and l < recent["low"].iloc[i+1] and
                l < recent["low"].iloc[i-2] and l < recent["low"].iloc[i+2]):
                if l < swing_low_val:
                    swing_low_val = l; swing_low_idx = i
        if swing_high_idx is None or swing_low_idx is None: return None, None, None
        rango = swing_high_val - swing_low_val
        if rango <= 0: return None, None, None
        if swing_high_idx > swing_low_idx:
            direccion = "LONG"
            niveles = {
                0.382: round(swing_high_val - rango * 0.382, 8),
                0.500: round(swing_high_val - rango * 0.500, 8),
                0.618: round(swing_high_val - rango * 0.618, 8),
                0.786: round(swing_high_val - rango * 0.786, 8),
                0.886: round(swing_high_val - rango * 0.886, 8),
            }
        else:
            direccion = "SHORT"
            niveles = {
                0.382: round(swing_low_val + rango * 0.382, 8),
                0.500: round(swing_low_val + rango * 0.500, 8),
                0.618: round(swing_low_val + rango * 0.618, 8),
                0.786: round(swing_low_val + rango * 0.786, 8),
                0.886: round(swing_low_val + rango * 0.886, 8),
            }
        for nivel, valor in sorted(niveles.items(), key=lambda x: abs(price - x[1])):
            if abs(price - valor) / price <= 0.005:
                if nivel == 0.886:   desc = "Fib 0.886 — zona reversión profunda"
                elif nivel == 0.618: desc = "Fib 0.618 — Golden Ratio"
                elif nivel == 0.786: desc = "Fib 0.786 — retroceso profundo"
                elif nivel == 0.500: desc = "Fib 0.500 — equilibrio"
                else:                desc = "Fib 0.382 — retroceso moderado"
                return nivel, desc, direccion
        return None, None, None
    except:
        return None, None, None


def detect_hh_ll(df, lookback=50):
    try:
        price  = df["close"].iloc[-1]
        recent = df.iloc[-lookback:]
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
        if len(highs) >= 2:
            last_h, prev_h = highs[-1][1], highs[-2][1]
            if last_h > prev_h and abs(price - last_h) / price <= 0.008:
                return "HH", round(last_h, 6)
        if len(lows) >= 2:
            last_l, prev_l = lows[-1][1], lows[-2][1]
            if last_l < prev_l and abs(price - last_l) / price <= 0.008:
                return "LL", round(last_l, 6)
    except:
        pass
    return None, None


def detect_candle(df):
    patterns = []
    try:
        c, p  = df.iloc[-1], df.iloc[-2]
        body  = abs(c["close"] - c["open"])
        total = c["high"] - c["low"]
        if total == 0: return patterns
        upper = c["high"] - max(c["close"], c["open"])
        lower = min(c["close"], c["open"]) - c["low"]
        ratio = body / total
        if ratio < 0.1:                        patterns.append("Doji")
        if upper > body*2 and upper > lower*2: patterns.append("Pin bar bajista")
        if lower > body*2 and lower > upper*2: patterns.append("Pin bar alcista")
        if (c["close"] < c["open"] and p["close"] > p["open"] and
            c["open"] >= p["close"] and c["close"] <= p["open"]):
            patterns.append("Engulfing bajista")
        if (c["close"] > c["open"] and p["close"] < p["open"] and
            c["open"] <= p["close"] and c["close"] >= p["open"]):
            patterns.append("Engulfing alcista")
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
        return round(((df["high"]-df["low"])/df["close"]*100).iloc[-n:].mean(), 4)
    except:
        return 0.0


def calc_atr(df, period=14):
    """ATR como % del precio — compara volatilidad entre activos"""
    try:
        high  = df["high"]
        low   = df["low"]
        close = df["close"]
        prev_close = close.shift(1)
        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low  - prev_close).abs()
        ], axis=1).max(axis=1)
        atr     = tr.ewm(span=period, adjust=False).mean().iloc[-1]
        atr_pct = round(atr / close.iloc[-1] * 100, 3)
        return atr_pct
    except:
        return 0.0


def tiene_direccionalidad(df, period=14):
    """
    Filtra activos sin direccionalidad — evita operar en rango.
    Retorna False si el movimiento neto < 25% del ATR acumulado.
    """
    try:
        n           = min(period, len(df) - 1)
        precio_ini  = df["close"].iloc[-n]
        precio_fin  = df["close"].iloc[-1]
        mov_neto    = abs(precio_fin - precio_ini) / precio_ini * 100
        high        = df["high"]
        low         = df["low"]
        close       = df["close"]
        prev_close  = close.shift(1)
        tr          = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low  - prev_close).abs()
        ], axis=1).max(axis=1)
        atr_sum = tr.iloc[-n:].sum() / close.iloc[-1] * 100
        if atr_sum == 0: return True
        return (mov_neto / atr_sum) >= 0.25
    except:
        return True


def calcular_trailing_sl(direction, precio_actual, precio_entry, sl_original, atr_pct):
    """Trailing stop basado en ATR — mueve SL a favor cuando la operación gana"""
    try:
        riesgo = abs(precio_entry - sl_original)
        if riesgo == 0: return sl_original
        ganancia    = abs(precio_actual - precio_entry)
        multiplicador = ganancia / riesgo
        atr_abs     = precio_entry * (atr_pct / 100)
        if direction == "LONG":
            if multiplicador >= 3.5:
                nuevo_sl = precio_actual - atr_abs * 1.5
                return round(max(nuevo_sl, precio_entry + riesgo * 2.5), 6)
            elif multiplicador >= 2.5:
                return round(precio_entry + riesgo * 2.0, 6)
            elif multiplicador >= 1.5:
                return round(precio_entry + riesgo * 0.1, 6)
        else:
            if multiplicador >= 3.5:
                nuevo_sl = precio_actual + atr_abs * 1.5
                return round(min(nuevo_sl, precio_entry - riesgo * 2.5), 6)
            elif multiplicador >= 2.5:
                return round(precio_entry - riesgo * 2.0, 6)
            elif multiplicador >= 1.5:
                return round(precio_entry - riesgo * 0.1, 6)
        return sl_original
    except:
        return sl_original

# =============================================================================
#   SCORING Y CLASIFICACIÓN (idéntico al bot crypto)
# =============================================================================

def calc_score(direction, rsi, ob, fvg, structure, patterns, vol_high, near_sr, divergence, htf_bias):
    score, labels = 0, []
    rsi_extreme_short = direction == "SHORT" and rsi >= RSI_EXTREME
    rsi_extreme_long  = direction == "LONG"  and rsi <= (100 - RSI_EXTREME)

    if direction == "LONG":
        if rsi <= RSI_OVERSOLD:                    score += 1.5; labels.append("RSI sobrevendido (" + str(rsi) + ")")
        if ob:                                     score += 2;   labels.append("OB alcista")
        if fvg:                                    score += 1.5; labels.append("FVG alcista")
        if structure in ("BOS_BULL","CHoCH_BULL"): score += 1.5; labels.append("Estructura: " + structure)
        bull_c = [p for p in patterns if "alcista" in p or "Doji" in p]
        if bull_c:                                 score += 1;   labels.append(" | ".join(bull_c))
        if divergence == "BULLISH_DIV":            score += 1.5; labels.append("Divergencia RSI alcista")
        if htf_bias == "BULLISH":                  score += 2;   labels.append("HTF Diario alcista")
        elif htf_bias == "BEARISH":                score -= 1
    else:
        if rsi >= RSI_OVERBOUGHT:                  score += 1.5; labels.append("RSI sobrecomprado (" + str(rsi) + ")")
        if ob:                                     score += 2;   labels.append("OB bajista")
        if fvg:                                    score += 1.5; labels.append("FVG bajista")
        if structure in ("BOS_BEAR","CHoCH_BEAR"): score += 1.5; labels.append("Estructura: " + structure)
        bear_c = [p for p in patterns if "bajista" in p or "Doji" in p]
        if bear_c:                                 score += 1;   labels.append(" | ".join(bear_c))
        if divergence == "BEARISH_DIV":            score += 1.5; labels.append("Divergencia RSI bajista")
        if htf_bias == "BEARISH":                  score += 2;   labels.append("HTF Diario bajista")
        elif htf_bias == "BULLISH":                score -= 1

    if vol_high: score += 1; labels.append("Volumen elevado")
    if near_sr:  score += 1; labels.append("Precio en S/R clave")

    if rsi_extreme_short or rsi_extreme_long:
        score = max(score, MIN_SCORE)

    return round(min(max(score, 0), 10)), labels


def calc_sl_tp_fx(price, direction, support, resistance, df=None, symbol=""):
    """
    SL basado en swing high/low más relevante.
    Buffer dinámico según tipo de activo:
    - JPY pairs → 0.15% (precio ~150, 0.1% = 0.15 pips, muy ajustado)
    - Commodities (oro, petróleo) → 0.3% (movimientos más amplios)
    - Forex majors → 0.1%
    Máximo de distancia dinámico: 3% commodities, 1.5% forex
    TPs: R:R 1:2, 1:3.5, 1:5
    """
    # Determinar buffer y límites según activo
    sym_upper = symbol.upper()
    if "JPY" in sym_upper:
        buffer_pct = 0.0015  # 0.15%
        max_sl_pct = 0.02
    elif sym_upper in ("XAUUSD", "XAGUSD", "USOIL", "UKOIL"):
        buffer_pct = 0.003   # 0.3%
        max_sl_pct = 0.04
    else:
        buffer_pct = 0.001   # 0.1%
        max_sl_pct = 0.015

    # Riesgo mínimo según activo
    if sym_upper in ("XAUUSD", "XAGUSD", "USOIL", "UKOIL"):
        min_risk_pct = 0.005  # 0.5% para commodities
    else:
        min_risk_pct = 0.002  # 0.2% para forex

    sl = None

    if df is not None:
        try:
            if direction == "SHORT":
                recent = df.iloc[-50:]
                swing_highs = []
                for i in range(2, len(recent)-2):
                    if (recent["high"].iloc[i] > recent["high"].iloc[i-1] and
                        recent["high"].iloc[i] > recent["high"].iloc[i+1] and
                        recent["high"].iloc[i] > recent["high"].iloc[i-2] and
                        recent["high"].iloc[i] > recent["high"].iloc[i+2]):
                        swing_highs.append(recent["high"].iloc[i])
                highs_sobre_precio = [h for h in swing_highs if h > price]
                if highs_sobre_precio:
                    swing_h      = max(highs_sobre_precio)
                    sl_candidato = round(swing_h * (1 + buffer_pct), 6)
                    if (sl_candidato - price) / price <= max_sl_pct:
                        sl = sl_candidato
            else:
                recent = df.iloc[-50:]
                swing_lows = []
                for i in range(2, len(recent)-2):
                    if (recent["low"].iloc[i] < recent["low"].iloc[i-1] and
                        recent["low"].iloc[i] < recent["low"].iloc[i+1] and
                        recent["low"].iloc[i] < recent["low"].iloc[i-2] and
                        recent["low"].iloc[i] < recent["low"].iloc[i+2]):
                        swing_lows.append(recent["low"].iloc[i])
                lows_bajo_precio = [l for l in swing_lows if l < price]
                if lows_bajo_precio:
                    swing_l      = min(lows_bajo_precio)
                    sl_candidato = round(swing_l * (1 - buffer_pct), 6)
                    if (price - sl_candidato) / price <= max_sl_pct:
                        sl = sl_candidato
        except Exception as e:
            print("Error swing SL FX: " + str(e))
            sl = None

    # Fallback — máximo/mínimo de las últimas 10 velas
    if sl is None and df is not None:
        try:
            if direction == "LONG":
                sl = round(df["low"].iloc[-10:].min() * (1 - buffer_pct), 6)
            else:
                sl = round(df["high"].iloc[-10:].max() * (1 + buffer_pct), 6)
        except Exception as e:
            print("Error fallback SL FX: " + str(e))
            sl = None

    # Último fallback al S/R estático
    if sl is None:
        if direction == "LONG":
            sl = round(support  * (1 - buffer_pct), 6)
        else:
            sl = round(resistance * (1 + buffer_pct), 6)

    if direction == "LONG":
        risk = max(price - sl, price * min_risk_pct)
        tp1, tp2, tp3 = round(price+risk*2.0, 6), round(price+risk*3.5, 6), round(price+risk*5.0, 6)
    else:
        risk = max(sl - price, price * min_risk_pct)
        tp1, tp2, tp3 = round(price-risk*2.0, 6), round(price-risk*3.5, 6), round(price-risk*5.0, 6)

    return sl, tp1, tp2, tp3, round(abs(tp1-price)/risk, 1), round(abs(tp2-price)/risk, 1)

# =============================================================================
#   FORMATO MENSAJES TELEGRAM
# =============================================================================

def format_setup_fx(s, tf_label):
    d         = s["direction"]
    is_long   = d == "LONG"
    htf_label = "ALCISTA" if s["htf_bias"] == "BULLISH" else ("BAJISTA" if s["htf_bias"] == "BEARISH" else "NEUTRAL")
    htf_emoji = "🟢" if s["htf_bias"] == "BULLISH" else ("🔴" if s["htf_bias"] == "BEARISH" else "⚪")
    stars     = "⭐" * min(int(s["score"]), 5)
    header    = ("🟢 <b>LONG 📈 — " if is_long else "🔴 <b>SHORT 📉 — ") + s["symbol"] + "</b>"

    lines = [
        "",
        header + " [" + tf_label + "]",
        "Score: <b>" + str(s["score"]) + "/10</b> " + stars,
        "💰 Precio: " + fmt_fx(s["price"]),
        "📊 RSI:    " + str(s["rsi"]),
        htf_emoji + " HTF 1D: " + htf_label,
        "🌊 Volatilidad: " + str(s["volatility"]) + "%",
    ]

    if s["divergence"]:
        div_emoji = "📈" if s["divergence"] == "BULLISH_DIV" else "📉"
        lines.append(div_emoji + " Divergencia: " + ("Alcista" if s["divergence"] == "BULLISH_DIV" else "Bajista"))
    if s["ob"]:
        ob_emoji = "🟩" if is_long else "🟥"
        lines.append(ob_emoji + " OB: " + fmt_fx(s["ob"]["low"]) + " - " + fmt_fx(s["ob"]["high"]))
    if s["fvg"]:
        lines.append("⬜ FVG: " + fmt_fx(s["fvg"]["low"]) + " - " + fmt_fx(s["fvg"]["high"]))
    if s["structure"]:
        lines.append("🔷 Estructura: " + s["structure"])

    hh_ll_type  = s.get("hh_ll_type")
    hh_ll_level = s.get("hh_ll_level")
    if hh_ll_type == "HH" and hh_ll_level:
        lines.append("🔺 HH: " + fmt_fx(hh_ll_level) + " — zona de oferta")
    elif hh_ll_type == "LL" and hh_ll_level:
        lines.append("🔻 LL: " + fmt_fx(hh_ll_level) + " — zona de demanda")

    fib_nivel = s.get("fib_nivel")
    fib_desc  = s.get("fib_desc")
    if fib_nivel and fib_desc:
        lines.append("🌀 " + fib_desc)

    lines.append("🕯 Vela: " + (" | ".join(s["candles"]) if s["candles"] else "Sin patron"))
    lines += [
        "🟢 Soporte:    " + fmt_fx(s["support"]),
        "🔴 Resistencia: " + fmt_fx(s["resistance"]),
        "━━━━━━━━━━━━━━━",
    ]

    ob  = s.get("ob")
    fvg = s.get("fvg")
    if ob:
        entrada = round((ob["high"] + ob["low"]) / 2, 6)
        lines.append("🟡 Entrada: " + fmt_fx(entrada) + " (zona OB)")
    elif fvg:
        entrada = round((fvg["high"] + fvg["low"]) / 2, 6)
        lines.append("🟡 Entrada: " + fmt_fx(entrada) + " (zona FVG)")
    else:
        lines.append("🟡 Entrada: " + fmt_fx(s["price"]) + " (precio actual)")

    lines += [
        "🛑 SL:  " + fmt_fx(s["sl"]),
        "🎯 TP1: " + fmt_fx(s["tp1"]) + " (R:R 1:" + str(s["rr1"]) + ")",
        "🎯 TP2: " + fmt_fx(s["tp2"]) + " (R:R 1:" + str(s["rr2"]) + ")",
        "🎯 TP3: " + fmt_fx(s["tp3"]),
        "✅ Confluencias:",
    ]
    for l in s["labels"]:
        lines.append("   • " + l)

    return "\n".join(lines)

# =============================================================================
#   ANÁLISIS POR SÍMBOLO
# =============================================================================

def analyze_symbol_fx(symbol, symbol_yf, interval, tf_label):
    """Analiza un par Forex/Commodity y retorna lista de setups válidos"""
    # Filtro de sesión específico por par — no operar fuera de horario de liquidez
    if not en_sesion_par(symbol):
        return None

    df = get_klines_fx(symbol_yf, interval, limit=200)
    if df is None: return None
    try:
        price                    = df["close"].iloc[-1]
        rsi                      = calc_rsi(df["close"])
        sup, res                 = calc_sr(df)
        ob_b, ob_s               = detect_ob(df)
        fv_b, fv_s               = detect_fvg(df)
        structure                = detect_structure(df)
        candles                  = detect_candle(df)
        vol_r, vol_h             = calc_vol(df)
        vol                      = calc_volatility(df)
        atr_pct                  = calc_atr(df)
        direccional              = tiene_direccionalidad(df)
        divergence               = detect_rsi_divergence(df)
        htf_bias                 = get_htf_bias_fx(symbol_yf)
        fib_nivel, fib_desc, fib_dir = detect_fibonacci(df)
        hh_ll_type, hh_ll_level  = detect_hh_ll(df)
        near_sup                 = abs(price - sup) / price < 0.005
        near_res                 = abs(price - res) / price < 0.005

        condicion_mercado = "TENDENCIA" if direccional else "RANGO"
        if atr_pct > 1.0: condicion_mercado = "ALTA_VOLATILIDAD"

        results = []
        for direction, ob, fvg, near in [("LONG", ob_b, fv_b, near_sup), ("SHORT", ob_s, fv_s, near_res)]:

            # ── FILTROS ──────────────────────────────────────────────────────

            # 0. Filtro ATR — no operar sin direccionalidad
            if not direccional: continue

            # 1. RSI direccional
            if direction == "SHORT" and rsi < 50 and rsi < RSI_EXTREME: continue
            if direction == "LONG"  and rsi > 50 and rsi > (100-RSI_EXTREME): continue

            # 2. HTF Diario — OPCIÓN C: solo operar EN DIRECCIÓN del bias mayor
            # HTF BAJISTA → solo SHORTs | HTF ALCISTA → solo LONGs
            # NEUTRAL → ambas | RSI extremo → excepción
            rsi_extremo = (direction == "SHORT" and rsi >= RSI_EXTREME) or \
                          (direction == "LONG"  and rsi <= (100-RSI_EXTREME))
            if not rsi_extremo:
                if htf_bias == "BEARISH" and direction == "LONG":  continue
                if htf_bias == "BULLISH" and direction == "SHORT": continue

            # 3. Score
            score, labels = calc_score(direction, rsi, ob, fvg, structure, candles, vol_h, near, divergence, htf_bias)

            # Fibonacci
            fib_valido = fib_nivel is not None and fib_dir == direction
            if fib_valido:
                if fib_nivel == 0.886:   score = min(score + 2.5, 10); labels.append(fib_desc)
                elif fib_nivel == 0.618: score = min(score + 2.0, 10); labels.append(fib_desc)
                elif fib_nivel == 0.786: score = min(score + 1.5, 10); labels.append(fib_desc)
                else:                    score = min(score + 1.0, 10); labels.append(fib_desc)

            # 4. Vela contradictoria
            if direction == "LONG"  and any("bajista" in p for p in candles): continue
            if direction == "SHORT" and any("alcista" in p for p in candles): continue

            # 5. Divergencia contraria
            if direction == "SHORT" and divergence == "BULLISH_DIV": continue
            if direction == "LONG"  and divergence == "BEARISH_DIV": continue

            # 6. Estructura contraria
            if direction == "LONG"  and structure in ("BOS_BEAR", "CHoCH_BEAR"): continue
            if direction == "SHORT" and structure in ("BOS_BULL", "CHoCH_BULL"): continue

            # 7. OB — precio cerca
            if ob:
                if direction == "SHORT" and (ob["low"] - price) / price > 0.005: continue
                if direction == "LONG"  and (price - ob["high"]) / price > 0.005: continue

            # 8. Zona obligatoria
            hh_ll_valido = (direction == "SHORT" and hh_ll_type == "HH") or \
                           (direction == "LONG"  and hh_ll_type == "LL")
            if not ob and not fvg and not hh_ll_valido: continue
            if hh_ll_valido:
                score = min(score + 2, 10)
                labels.append(("HH " if direction == "SHORT" else "LL ") + fmt_fx(hh_ll_level))

            # 9. S/R
            if direction == "SHORT" and near_sup: continue
            if direction == "LONG"  and near_res: continue

            # 10. Score mínimo
            if score < MIN_SCORE: continue

            # 11. Cooldown
            if ya_alerte_fx(symbol, direction, tf_label): continue

            # SL/TP
            sl, tp1, tp2, tp3, rr1, rr2 = calc_sl_tp_fx(price, direction, sup, res, df, symbol)

            results.append({
                "symbol":      symbol,
                "direction":   direction,
                "price":       price,
                "rsi":         rsi,
                "score":       score,
                "labels":      labels,
                "support":     sup,
                "resistance":  res,
                "ob":          ob,
                "fvg":         fvg,
                "structure":   structure,
                "candles":     candles,
                "vol_ratio":   vol_r,
                "divergence":  divergence,
                "htf_bias":    htf_bias,
                "sl":          sl,
                "tp1":         tp1,
                "tp2":         tp2,
                "tp3":               tp3,
                "rr1":               rr1,
                "rr2":               rr2,
                "volatility":        vol,
                "hh_ll_type":        hh_ll_type,
                "hh_ll_level":       hh_ll_level,
                "fib_nivel":         fib_nivel if fib_valido else None,
                "fib_desc":          fib_desc  if fib_valido else None,
                "condicion_mercado": condicion_mercado,
                "atr_pct":           atr_pct,
            })

        return results if results else None
    except Exception as e:
        print("Error FX " + symbol + ": " + str(e))
        return None

# =============================================================================
#   ESCANEO PRINCIPAL
# =============================================================================

def scan_all_fx():
    """Escanea todos los pares Forex/Commodity"""

    # Solo operar en sesión activa (Londres + Nueva York)
    if not en_sesion_activa():
        hora = datetime.now(ARG_TZ).strftime("%H:%M")
        print("[" + hora + "] Fuera de sesión — sin escaneo")
        return

    now = datetime.now(ARG_TZ).strftime("%H:%M")
    ts  = datetime.now(ARG_TZ).strftime("%d/%m %H:%M:%S")
    print("[" + now + "] Escaneando Forex/Commodities...")

    all_setups = []
    for symbol, symbol_yf in SYMBOLS.items():
        for interval, tf_label, _, _ in INTERVALS:
            try:
                results = analyze_symbol_fx(symbol, symbol_yf, interval, tf_label)
                if results:
                    for r in results:
                        r["tf_label"] = tf_label
                    all_setups.extend(results)
            except Exception as e:
                print("Error " + symbol + " " + interval + ": " + str(e))
            time.sleep(0.5)  # yfinance necesita un poco más de tiempo entre requests

    all_setups.sort(key=lambda x: x["score"], reverse=True)

    msg  = "💱 <b>FOREX SCANNER v1.0 — " + now + "</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += "📋 Pares: " + str(len(SYMBOLS)) + " | Setups: " + str(len(all_setups)) + "\n"
    msg += "⚙️ Score mín: " + str(MIN_SCORE) + "/10\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"

    if all_setups:
        longs  = [s for s in all_setups if s["direction"] == "LONG"]
        shorts = [s for s in all_setups if s["direction"] == "SHORT"]
        if longs:
            msg += "\n🟢 <b>SETUPS LONG</b>\n"
            for s in longs:
                guardar_alerta_fx(s, s["tf_label"])
                msg += format_setup_fx(s, s["tf_label"]) + "\n"
        if shorts:
            msg += "\n🔴 <b>SETUPS SHORT</b>\n"
            for s in shorts:
                guardar_alerta_fx(s, s["tf_label"])
                msg += format_setup_fx(s, s["tf_label"]) + "\n"
    else:
        msg += "\n⏳ Sin setups ahora.\n"

    msg += "\n🕐 " + ts
    msg += "\n⚠️ No es consejo financiero."
    send_telegram(msg)
    print("[" + now + "] FX scan completado — " + str(len(all_setups)) + " setups")

# =============================================================================
#   RESUMEN DIARIO
# =============================================================================

def resumen_diario_fx():
    historial = get_historial_fx(20)
    if not historial: return
    msg  = "📅 <b>RESUMEN FOREX — " + datetime.now(ARG_TZ).strftime("%d/%m/%Y") + "</b>\n\n"
    msg += "Últimas " + str(len(historial)) + " alertas:\n"
    for row in historial:
        symbol, direction, timeframe, precio, score, enviada_at = row
        emoji = "🟢" if direction == "LONG" else "🔴"
        msg += emoji + " " + ("Long" if direction == "LONG" else "Short") + " " + symbol
        msg += " [" + timeframe + "] Score:" + str(score)
        msg += " @ " + fmt_fx(precio)
        msg += " — " + enviada_at.strftime("%d/%m %H:%M") + "\n"
    send_telegram(msg)

# =============================================================================
#   INICIO
# =============================================================================

if __name__ == "__main__":
    print("Forex Scanner Bot v1.0 iniciado...")
    init_db()
    send_telegram(
        "<b>💱 Forex Scanner Bot v1.0 ACTIVO</b>\n\n"
        "📊 Pares:\n"
        "— Forex: EUR/USD, GBP/USD, USD/JPY, AUD/USD, USD/CHF, NZD/USD, USD/CAD, EUR/GBP\n"
        "— Commodities: Oro, Plata, Petróleo WTI, Brent\n\n"
        "⏰ Horario de operación:\n"
        "— Lun-Jue: 00:00–17:00 y 19:00–24:00 ART\n"
        "— Viernes: 00:00–17:00 ART\n"
        "— Sábado: CERRADO\n"
        "— Domingo: desde 19:00 ART\n\n"
        "🔄 Escaneo cada 15 minutos\n"
        "⚙️ Score mínimo: " + str(MIN_SCORE) + "/10\n\n"
        "💬 Comandos: /fxresumen | /fxscan | /fxreporte | /fxayuda"
    )
    scan_all_fx()
    schedule.every(15).minutes.do(scan_all_fx)
    schedule.every(15).minutes.do(verificar_resultados_fx)
    schedule.every().day.at("08:00").do(resumen_diario_fx)

    last_update_id = None
    while True:
        schedule.run_pending()
        last_update_id = procesar_comandos(last_update_id)
        time.sleep(30)
