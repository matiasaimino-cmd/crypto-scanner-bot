# =============================================================================
#   CRYPTO SCANNER BOT v4.3
#   Autor: matiasaimino-cmd
#   Descripción: Scanner de crypto con análisis técnico automatizado
#   Plataforma: Railway + PostgreSQL + Telegram
# =============================================================================

import requests
import pandas as pd
import numpy as np
import time
import schedule
import psycopg2
import os
import html as html_lib
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
BINANCE_BASE        = "https://api.binance.com/api/v3"

if not TELEGRAM_TOKEN or not CHAT_ID:
    raise SystemExit("❌ Faltan TELEGRAM_TOKEN o TELEGRAM_CHAT_ID en variables de entorno")

MIN_SCORE           = 6
RSI_OVERBOUGHT      = 70
RSI_OVERSOLD        = 30
RSI_EXTREME         = 75
ALERTA_COOLDOWN_MIN = 30

# Session HTTP con reintentos automáticos — evita colapso por errores 429/5xx
_session = requests.Session()
_retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
_session.mount("https://", HTTPAdapter(max_retries=_retries))

SYMBOLS = [
    "BTCUSDT",  "ETHUSDT",  "SOLUSDT",   "XRPUSDT",  "DOGEUSDT",
    "ADAUSDT",  "AVAXUSDT", "DOTUSDT",   "PEPEUSDT",  "LTCUSDT",
    "ATOMUSDT", "NEARUSDT", "HBARUSDT",  "THETAUSDT", "FTMUSDT",
    "SANDUSDT", "MANAUSDT", "RUNEUSDT",  "OPUSDT",    "RENDERUSDT"
]

INTERVALS = [
    ("15m", "Scalping 15m"),
    ("1h",  "Day Trading 1H")
]

# =============================================================================
#   BASE DE DATOS
# =============================================================================

def get_db():
    """Retorna conexión a PostgreSQL"""
    try:
        return psycopg2.connect(os.environ["DATABASE_URL"])
    except Exception as e:
        print("Error DB: " + str(e))
        return None


def init_db():
    """Crea las tablas si no existen y ajusta tipos de columnas"""
    conn = get_db()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS alertas (
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
                enviada_at   TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS market_state (
                id             SERIAL PRIMARY KEY,
                symbol         VARCHAR(20),
                timeframe      VARCHAR(20),
                rsi            FLOAT,
                estructura     TEXT,
                htf_bias       TEXT,
                divergencia    TEXT,
                precio         FLOAT,
                actualizado_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rsi_history (
                id            SERIAL PRIMARY KEY,
                symbol        VARCHAR(20),
                timeframe     VARCHAR(20),
                rsi           FLOAT,
                precio        FLOAT,
                registrado_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tracking (
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
                resultado     VARCHAR(10) DEFAULT 'OPEN',
                tp_alcanzado  INTEGER DEFAULT 0,
                precio_cierre FLOAT,
                pnl_pct       FLOAT,
                abierto_at    TIMESTAMP DEFAULT NOW(),
                cerrado_at    TIMESTAMP
            )
        """)
        conn.commit()
        # Limpiar duplicados en market_state y crear constraint único
        try:
            # Primero eliminar filas duplicadas dejando solo la más reciente
            cur.execute("""
                DELETE FROM market_state
                WHERE id NOT IN (
                    SELECT MAX(id)
                    FROM market_state
                    GROUP BY symbol, timeframe
                )
            """)
            conn.commit()
            # Ahora crear el constraint único
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conname = 'market_state_unique'
                    ) THEN
                        ALTER TABLE market_state
                        ADD CONSTRAINT market_state_unique UNIQUE (symbol, timeframe);
                    END IF;
                END$$;
            """)
            conn.commit()
            print("Constraint market_state_unique OK")
        except Exception as e:
            print("Error constraint: " + str(e))
            conn.rollback()
        # Ampliar columnas VARCHAR por si las tablas ya existían con tamaño menor
        try:
            cur.execute("ALTER TABLE alertas      ALTER COLUMN timeframe TYPE VARCHAR(20)")
            cur.execute("ALTER TABLE market_state ALTER COLUMN timeframe TYPE VARCHAR(20)")
            cur.execute("ALTER TABLE rsi_history  ALTER COLUMN timeframe TYPE VARCHAR(20)")
            conn.commit()
        except:
            conn.rollback()
        print("DB iniciada OK")
    except Exception as e:
        print("Error init DB: " + str(e))
    finally:
        conn.close()


def ya_alerte(symbol, direction, timeframe):
    """
    Verifica si ya se envió alerta del mismo par/dirección/timeframe
    en los últimos ALERTA_COOLDOWN_MIN minutos.
    Retorna True si hay cooldown activo (no enviar).
    """
    conn = get_db()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM alertas
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


def guardar_alerta(s, tf_label):
    """Guarda la alerta en DB — se llama DESPUÉS de pasar todos los filtros"""
    conn = get_db()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO alertas
                (symbol, direction, timeframe, precio, rsi, score, sl, tp1, tp2, tp3, confluencias)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            s["symbol"], s["direction"], tf_label,
            float(s["price"]),  float(s["rsi"]),  float(s["score"]),
            float(s["sl"]),     float(s["tp1"]),  float(s["tp2"]),  float(s["tp3"]),
            " | ".join(s["labels"])
        ))
        alerta_id = cur.fetchone()[0]
        conn.commit()

        # Crear registro de tracking para esta señal
        cur.execute("""
            INSERT INTO tracking
                (alerta_id, symbol, direction, timeframe, precio_entry, sl, tp1, tp2, tp3, score)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            alerta_id, s["symbol"], s["direction"], tf_label,
            float(s["price"]), float(s["sl"]),
            float(s["tp1"]),   float(s["tp2"]),  float(s["tp3"]),
            float(s["score"])
        ))
        conn.commit()
    except Exception as e:
        print("Error guardar alerta: " + str(e))
    finally:
        conn.close()


def verificar_resultados():
    """
    Verifica el resultado de las operaciones abiertas.
    Por cada señal OPEN consulta el precio actual y determina si:
    - Tocó SL → LOSS
    - Llegó a TP1, TP2 o TP3 → WIN
    - Pasaron más de 48hs sin resultado → EXPIRED
    Se ejecuta en cada ciclo de escaneo.
    """
    conn = get_db()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, symbol, direction, precio_entry, sl, tp1, tp2, tp3, abierto_at
            FROM tracking
            WHERE resultado = 'OPEN'
        """)
        operaciones = cur.fetchall()
        if not operaciones: return

        for op in operaciones:
            op_id, symbol, direction, entry, sl, tp1, tp2, tp3, abierto_at = op
            try:
                # Obtener precio actual de Binance
                r = _session.get(
                    BINANCE_BASE + "/ticker/price",
                    params={"symbol": symbol},
                    timeout=5
                )
                precio_actual = float(r.json()["price"])

                resultado     = None
                tp_alcanzado  = 0
                precio_cierre = precio_actual
                pnl_pct       = 0.0

                if direction == "LONG":
                    if precio_actual <= sl:
                        resultado = "LOSS"
                        pnl_pct   = round((precio_actual - entry) / entry * 100, 2)
                    elif precio_actual >= tp3:
                        resultado = "WIN"; tp_alcanzado = 3
                        pnl_pct   = round((precio_actual - entry) / entry * 100, 2)
                    elif precio_actual >= tp2:
                        resultado = "WIN"; tp_alcanzado = 2
                        pnl_pct   = round((precio_actual - entry) / entry * 100, 2)
                    elif precio_actual >= tp1:
                        resultado = "WIN"; tp_alcanzado = 1
                        pnl_pct   = round((precio_actual - entry) / entry * 100, 2)
                else:  # SHORT
                    if precio_actual >= sl:
                        resultado = "LOSS"
                        pnl_pct   = round((entry - precio_actual) / entry * 100, 2)
                    elif precio_actual <= tp3:
                        resultado = "WIN"; tp_alcanzado = 3
                        pnl_pct   = round((entry - precio_actual) / entry * 100, 2)
                    elif precio_actual <= tp2:
                        resultado = "WIN"; tp_alcanzado = 2
                        pnl_pct   = round((entry - precio_actual) / entry * 100, 2)
                    elif precio_actual <= tp1:
                        resultado = "WIN"; tp_alcanzado = 1
                        pnl_pct   = round((entry - precio_actual) / entry * 100, 2)

                # Expirar operaciones según timeframe
                # 15m → 24hs | 1H → 5 días | 4H → 10 días
                if resultado is None:
                    cur2 = conn.cursor()
                    cur2.execute("SELECT timeframe FROM tracking WHERE id = %s", (op_id,))
                    row = cur2.fetchone()
                    tf = row[0] if row else ""
                    if "15m" in tf or "Scalping" in tf:
                        max_horas = 24
                    elif "4H" in tf or "4h" in tf:
                        max_horas = 240
                    else:
                        max_horas = 120  # 1H → 5 días

                    horas = (datetime.now(ARG_TZ) - abierto_at.replace(tzinfo=ARG_TZ)).total_seconds() / 3600
                    if horas > max_horas:
                        resultado     = "EXPIRED"
                        precio_cierre = precio_actual
                        if direction == "LONG":
                            pnl_pct = round((precio_actual - entry) / entry * 100, 2)
                        else:
                            pnl_pct = round((entry - precio_actual) / entry * 100, 2)

                # Actualizar en DB si hay resultado
                if resultado:
                    cur.execute("""
                        UPDATE tracking
                        SET resultado     = %s,
                            tp_alcanzado  = %s,
                            precio_cierre = %s,
                            pnl_pct       = %s,
                            cerrado_at    = NOW()
                        WHERE id = %s
                    """, (resultado, tp_alcanzado, precio_cierre, pnl_pct, op_id))
                    conn.commit()

                    # Notificar por Telegram si es WIN o LOSS
                    if resultado in ("WIN", "LOSS"):
                        emoji  = "✅" if resultado == "WIN" else "❌"
                        emoji2 = "🟢" if direction == "LONG" else "🔴"
                        msg    = emoji + " <b>" + resultado + "</b> — " + emoji2 + " " + direction + " " + symbol + "\n"
                        if resultado == "WIN":
                            msg += "🎯 TP" + str(tp_alcanzado) + " alcanzado\n"
                        else:
                            msg += "🛑 SL tocado\n"
                        msg += "📊 Entrada: " + fmt(entry) + " → " + fmt(precio_cierre) + "\n"
                        msg += "💰 P&L: <b>" + ("+" if pnl_pct > 0 else "") + str(pnl_pct) + "%</b>"
                        send_telegram(msg)
                        print("Tracking: " + resultado + " " + symbol + " " + str(pnl_pct) + "%")

            except Exception as e:
                print("Error verificar " + symbol + ": " + str(e))
                continue

    except Exception as e:
        print("Error verificar resultados: " + str(e))
    finally:
        conn.close()


def reporte_tracking():
    """
    Genera reporte de rendimiento con todas las operaciones cerradas.
    Comando: /reporte
    """
    conn = get_db()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT resultado, COUNT(*), AVG(pnl_pct), MAX(pnl_pct), MIN(pnl_pct)
            FROM tracking
            WHERE resultado != 'OPEN'
            GROUP BY resultado
        """)
        stats = cur.fetchall()

        cur.execute("""
            SELECT symbol, direction, timeframe, precio_entry, pnl_pct, resultado, tp_alcanzado, abierto_at
            FROM tracking
            WHERE resultado != 'OPEN'
            ORDER BY abierto_at DESC
            LIMIT 20
        """)
        ultimas = cur.fetchall()

        if not stats:
            send_telegram("📊 Sin operaciones cerradas todavía.")
            return

        # Calcular métricas generales
        wins     = next((r for r in stats if r[0] == "WIN"),  (None, 0, 0, 0, 0))
        losses   = next((r for r in stats if r[0] == "LOSS"), (None, 0, 0, 0, 0))
        total    = sum(r[1] for r in stats if r[0] in ("WIN", "LOSS"))
        win_rate = round(wins[1] / total * 100, 1) if total > 0 else 0

        msg  = "📊 <b>REPORTE DE RENDIMIENTO</b>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━\n"
        msg += "Total operaciones: <b>" + str(total) + "</b>\n"
        msg += "✅ Ganadoras: <b>" + str(wins[1]) + "</b> (" + str(win_rate) + "%)\n"
        msg += "❌ Perdedoras: <b>" + str(losses[1]) + "</b>\n"
        if wins[2]: msg += "💰 P&L prom WIN:  <b>+" + str(round(wins[2], 2)) + "%</b>\n"
        if losses[2]: msg += "💸 P&L prom LOSS: <b>" + str(round(losses[2], 2)) + "%</b>\n"
        msg += "\n📋 <b>Últimas 20 operaciones:</b>\n"

        for row in ultimas:
            symbol, direction, tf, entry, pnl, resultado, tp, fecha = row
            if resultado == "WIN":
                emoji = "✅ TP" + str(tp)
            elif resultado == "LOSS":
                emoji = "❌ SL"
            else:
                emoji = "⏰ EXP"
            dir_emoji = "🟢" if direction == "LONG" else "🔴"
            pnl_str   = ("+" if pnl and pnl > 0 else "") + str(round(pnl, 2)) + "%" if pnl else "?"
            msg += dir_emoji + " " + symbol + " " + emoji + " " + pnl_str
            msg += " — " + fecha.strftime("%d/%m %H:%M") + "\n"

        send_telegram(msg)
    except Exception as e:
        print("Error reporte tracking: " + str(e))
    finally:
        conn.close()


def guardar_estado(symbol, timeframe, rsi, estructura, htf_bias, divergencia, precio):
    """Guarda el estado actual del mercado — actualiza si ya existe"""
    conn = get_db()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO market_state
                (symbol, timeframe, rsi, estructura, htf_bias, divergencia, precio)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, timeframe) DO UPDATE
            SET rsi          = EXCLUDED.rsi,
                estructura   = EXCLUDED.estructura,
                htf_bias     = EXCLUDED.htf_bias,
                divergencia  = EXCLUDED.divergencia,
                precio       = EXCLUDED.precio,
                actualizado_at = NOW()
        """, (symbol, timeframe, float(rsi), str(estructura), str(htf_bias), str(divergencia), float(precio)))
        conn.commit()
    except Exception as e:
        print("Error guardar estado: " + str(e))
    finally:
        conn.close()


def guardar_rsi(symbol, timeframe, rsi, precio):
    """Guarda historial de RSI — mantiene las últimas 200 entradas por par"""
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


def get_historial_alertas(limite=20):
    """Obtiene las últimas alertas para el resumen diario"""
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

# =============================================================================
#   TELEGRAM
# =============================================================================

def send_telegram(message):
    """Envía mensaje a Telegram en chunks de 4000 caracteres con reintentos"""
    url = "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/sendMessage"
    for chunk in [message[i:i+4000] for i in range(0, len(message), 4000)]:
        try:
            _session.post(
                url,
                json={"chat_id": CHAT_ID, "text": chunk, "parse_mode": "HTML"},
                timeout=10
            )
            time.sleep(0.5)
        except Exception as e:
            print("Error Telegram: " + str(e))


def get_telegram_updates(offset=None):
    """Obtiene mensajes nuevos de Telegram"""
    try:
        url    = "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/getUpdates"
        params = {"timeout": 5}
        if offset: params["offset"] = offset
        r = _session.get(url, params=params, timeout=10)
        return r.json().get("result", [])
    except:
        return []


def procesar_comandos(last_update_id):
    """
    Procesa comandos enviados por Telegram.
    /resumen → últimas 20 alertas
    /scan    → escaneo inmediato
    /ayuda   → lista de comandos
    """
    updates = get_telegram_updates(offset=last_update_id)
    for update in updates:
        last_update_id = update["update_id"] + 1
        msg  = update.get("message", {})
        text = msg.get("text", "").strip().lower()
        chat = str(msg.get("chat", {}).get("id", ""))
        if chat != CHAT_ID: continue
        if text == "/resumen":
            print("Comando /resumen recibido")
            resumen_diario()
        elif text == "/scan":
            print("Comando /scan recibido")
            send_telegram("🔄 Iniciando escaneo manual...")
            scan_all()
        elif text == "/backtest":
            print("Comando /backtest recibido")
            run_backtest()
        elif text == "/reporte":
            print("Comando /reporte recibido")
            reporte_tracking()
        elif text == "/ayuda":
            send_telegram(
                "🤖 <b>Comandos disponibles:</b>\n\n"
                "/resumen   — últimas 20 alertas\n"
                "/scan      — escaneo inmediato\n"
                "/backtest  — backtesting del último mes\n"
                "/reporte   — rendimiento de operaciones\n"
                "/ayuda     — esta ayuda"
            )
    return last_update_id

# =============================================================================
#   DATOS — BINANCE API
# =============================================================================

def get_klines(symbol, interval, limit=200):
    """Obtiene velas de Binance usando session con reintentos automáticos"""
    try:
        r = _session.get(
            BINANCE_BASE + "/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
            timeout=10
        )
        data = r.json()
        if not isinstance(data, list) or len(data) < 10:
            return None
        df = pd.DataFrame(data, columns=[
            "time", "open", "high", "low", "close", "volume",
            "close_time", "quote_vol", "trades", "taker_base", "taker_quote", "ignore"
        ])
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna().reset_index(drop=True)
        return df if len(df) >= 30 else None
    except:
        return None


def fmt(p):
    """Formatea precio según su magnitud"""
    try:
        if p >= 100: return "$" + "{:,.2f}".format(p)
        elif p >= 1: return "$" + "{:.4f}".format(p)
        else:        return "$" + "{:.6f}".format(p)
    except:
        return str(p)

# =============================================================================
#   INDICADORES TÉCNICOS
# =============================================================================

def calc_rsi(closes, period=14):
    """
    RSI con media de Wilder (EMA alpha=1/period).
    Mismo método que TradingView.
    """
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
    """
    Detecta divergencias de RSI.
    BULLISH_DIV: precio LL pero RSI HL → señal alcista.
    BEARISH_DIV: precio HH pero RSI LH → señal bajista.
    """
    try:
        closes   = df["close"]
        delta    = closes.diff()
        gain     = delta.where(delta > 0, 0.0)
        loss     = (-delta.where(delta < 0, 0.0))
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
                price_highs.append((i, ph))
                rsi_highs.append((i, rh))
            if (pl < recent_df["low"].iloc[i-1] and pl < recent_df["low"].iloc[i+1] and
                pl < recent_df["low"].iloc[i-2] and pl < recent_df["low"].iloc[i+2]):
                price_lows.append((i, pl))
                rsi_lows.append((i, rh))

        divergence = None
        if (len(price_highs) >= 2 and
            price_highs[-1][1] > price_highs[-2][1] and
            rsi_highs[-1][1]  < rsi_highs[-2][1] and
            rsi_highs[-1][1]  > 50):
            divergence = "BEARISH_DIV"
        if (len(price_lows) >= 2 and
            price_lows[-1][1] < price_lows[-2][1] and
            rsi_lows[-1][1]   > rsi_lows[-2][1] and
            rsi_lows[-1][1]   < 50):
            divergence = "BULLISH_DIV"
        return divergence
    except:
        return None


def get_btc_momentum(interval):
    """
    Detecta el momentum actual de BTC.
    Si BTC sube → no dar SHORTs en altcoins.
    Si BTC baja → no dar LONGs en altcoins.
    Retorna: BULLISH | BEARISH | NEUTRAL
    """
    try:
        df    = get_klines("BTCUSDT", interval, limit=30)
        if df is None: return "NEUTRAL"
        price = df["close"].iloc[-1]
        prev3 = df["close"].iloc[-4]
        ema20 = df["close"].ewm(span=20, adjust=False, min_periods=10).mean().iloc[-1]
        cambio = (price - prev3) / prev3 * 100
        bull, bear = 0, 0
        if cambio > 0.3:    bull += 2
        elif cambio < -0.3: bear += 2
        if price > ema20: bull += 1
        else:             bear += 1
        last = df.iloc[-1]
        if last["close"] > last["open"]: bull += 1
        else:                            bear += 1
        if bull > bear:   return "BULLISH"
        elif bear > bull: return "BEARISH"
        return "NEUTRAL"
    except:
        return "NEUTRAL"


def get_htf_bias(symbol, interval):
    """
    Sesgo del marco temporal mayor (HTF).
    Para 15m/1h usa 4H. Evalúa EMA50, EMA200 y estructura.
    Retorna: BULLISH | BEARISH | NEUTRAL
    """
    try:
        next_tf = "4h" if interval in ("15m", "1h") else "1d"
        df      = get_klines(symbol, next_tf, limit=250)
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
            if df["low"].iloc[i]  < df["low"].iloc[i-1]  and df["low"].iloc[i]  < df["low"].iloc[i+1]:
                lows.append(df["low"].iloc[i])
        if len(highs) >= 2 and len(lows) >= 2:
            if highs[-1] > highs[-2] and lows[-1] > lows[-2]:   bull += 2
            elif highs[-1] < highs[-2] and lows[-1] < lows[-2]: bear += 2
        if bull > bear:   return "BULLISH"
        elif bear > bull: return "BEARISH"
        return "NEUTRAL"
    except:
        return "NEUTRAL"


def get_1h_bias(symbol):
    """
    Sesgo del 1H — filtro intermedio para trades de 15m.
    Retorna: BULLISH | BEARISH | NEUTRAL
    """
    try:
        df    = get_klines(symbol, "1h", limit=50)
        if df is None: return "NEUTRAL"
        closes = df["close"]
        price  = closes.iloc[-1]
        ema50  = closes.ewm(span=50, adjust=False, min_periods=20).mean().iloc[-1]
        bull, bear = 0, 0
        if price > ema50: bull += 1
        else:             bear += 1
        highs, lows = [], []
        for i in range(2, len(df) - 2):
            if df["high"].iloc[i] > df["high"].iloc[i-1] and df["high"].iloc[i] > df["high"].iloc[i+1]:
                highs.append(df["high"].iloc[i])
            if df["low"].iloc[i]  < df["low"].iloc[i-1]  and df["low"].iloc[i]  < df["low"].iloc[i+1]:
                lows.append(df["low"].iloc[i])
        if len(highs) >= 2 and len(lows) >= 2:
            if highs[-1] > highs[-2] and lows[-1] > lows[-2]:   bull += 2
            elif highs[-1] < highs[-2] and lows[-1] < lows[-2]: bear += 2
            lc = df["close"].iloc[-1]
            if lc > highs[-1]: bull += 1
            if lc < lows[-1]:  bear += 1
        if bull > bear:   return "BULLISH"
        elif bear > bull: return "BEARISH"
        return "NEUTRAL"
    except:
        return "NEUTRAL"


def calc_sr(df):
    """
    Soporte y resistencia más cercanos al precio.
    Usa pivots de 5 velas + mínimos/máximos recientes + clustering.
    """
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

        def cluster(levels):
            if not levels: return levels
            clustered = [levels[0]]
            for l in levels[1:]:
                if abs(l - clustered[-1]) / price > 0.003:
                    clustered.append(l)
            return clustered

        sups = cluster(sups)
        ress = cluster(ress)
        return (round(sups[0], 6) if sups else round(price * 0.98, 6),
                round(ress[0], 6) if ress else round(price * 1.02, 6))
    except:
        price = df["close"].iloc[-1]
        return round(price * 0.98, 6), round(price * 1.02, 6)


def detect_ob(df):
    """
    Order Blocks alcistas y bajistas. Busca el más reciente.
    OB alcista: vela bajista + impulso alcista, precio dentro.
    OB bajista: vela alcista + impulso bajista, precio dentro.
    """
    ob_bull, ob_bear = None, None
    try:
        price = df["close"].iloc[-1]
        for i in range(min(len(df)-3, 200), 4, -1):
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
    """
    Fair Value Gaps. Busca el más reciente.
    FVG alcista: gap entre high[i-2] y low[i], precio dentro.
    FVG bajista: gap entre low[i-2] y high[i], precio dentro.
    """
    fvg_bull, fvg_bear = None, None
    try:
        price     = df["close"].iloc[-1]
        search_df = df.iloc[-100:]
        for i in range(len(search_df)-1, 1, -1):
            if fvg_bull is None and search_df["low"].iloc[i] > search_df["high"].iloc[i-2]:
                gl, gh   = search_df["high"].iloc[i-2], search_df["low"].iloc[i]
                gap_size = (gh - gl) / price * 100
                if gap_size > 0.05 and gl <= price <= gh:
                    fvg_bull = {"high": round(gh, 6), "low": round(gl, 6), "size": round(gap_size, 3)}
            if fvg_bear is None and search_df["high"].iloc[i] < search_df["low"].iloc[i-2]:
                gh, gl   = search_df["low"].iloc[i-2], search_df["high"].iloc[i]
                gap_size = (gh - gl) / price * 100
                if gap_size > 0.05 and gl <= price <= gh:
                    fvg_bear = {"high": round(gh, 6), "low": round(gl, 6), "size": round(gap_size, 3)}
            if fvg_bull and fvg_bear: break
    except:
        pass
    return fvg_bull, fvg_bear


def detect_structure(df):
    """
    Detecta BOS (Break of Structure) y CHoCH (Change of Character).
    Verifica las últimas 5 velas para no perder eventos recientes.
    Solo considera niveles de las últimas 50 velas.
    """
    try:
        highs, lows = [], []
        for i in range(2, len(df)-2):
            if df["high"].iloc[i] > df["high"].iloc[i-1] and df["high"].iloc[i] > df["high"].iloc[i+1]:
                highs.append((i, df["high"].iloc[i]))
            if df["low"].iloc[i]  < df["low"].iloc[i-1]  and df["low"].iloc[i]  < df["low"].iloc[i+1]:
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
    """
    Detecta si el precio está en un nivel de retroceso de Fibonacci.
    Calcula desde el último swing significativo.

    Niveles y pesos en el score:
    0.382 → +1.0  (retroceso moderado)
    0.500 → +1.0  (equilibrio)
    0.618 → +2.0  (Golden Ratio — el más importante)
    0.786 → +1.5  (retroceso profundo)
    0.886 → +2.5  (zona de reversión profunda — falso breakout)

    Retorna: (nivel, descripcion, direccion) o (None, None, None)
    Tolerancia: 0.5% del nivel
    """
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
                    swing_high_val = h
                    swing_high_idx = i
            if (l < recent["low"].iloc[i-1] and l < recent["low"].iloc[i+1] and
                l < recent["low"].iloc[i-2] and l < recent["low"].iloc[i+2]):
                if l < swing_low_val:
                    swing_low_val = l
                    swing_low_idx = i

        if swing_high_idx is None or swing_low_idx is None: return None, None, None
        rango = swing_high_val - swing_low_val
        if rango <= 0: return None, None, None

        if swing_high_idx > swing_low_idx:
            # Movimiento alcista → retroceso bajista → buscar LONG en el retroceso
            direccion = "LONG"
            niveles = {
                0.382: round(swing_high_val - rango * 0.382, 8),
                0.500: round(swing_high_val - rango * 0.500, 8),
                0.618: round(swing_high_val - rango * 0.618, 8),
                0.786: round(swing_high_val - rango * 0.786, 8),
                0.886: round(swing_high_val - rango * 0.886, 8),
            }
        else:
            # Movimiento bajista → retroceso alcista → buscar SHORT en el retroceso
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
    """
    Detecta si el precio está cerca de un HH o LL reciente.
    HH = zona de oferta → ideal para SHORT.
    LL = zona de demanda → ideal para LONG.
    Tolerancia: 0.8%.
    """
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
    """Patrones de vela: Doji, Pin bar, Engulfing."""
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
    """
    Ratio volumen actual vs promedio últimas 20 velas.
    Elevado = 150%+. Retorna: (ratio%, es_elevado)
    """
    try:
        avg  = df["volume"].iloc[-21:-1].mean()
        last = df["volume"].iloc[-1]
        if avg == 0: return 100, False
        ratio = round((last/avg)*100)
        return ratio, ratio >= 150
    except:
        return 100, False


def calc_volatility(df):
    """Volatilidad promedio (% rango high-low) últimas 20 velas"""
    try:
        if len(df) < 5: return 0.0
        n = min(20, len(df))
        return round(((df["high"]-df["low"])/df["close"]*100).iloc[-n:].mean(), 2)
    except:
        return 0.0

# =============================================================================
#   SCORING Y CLASIFICACIÓN
# =============================================================================

def clasificar_setup(direction, rsi, structure, divergence, htf_bias):
    """
    Tipo de setup:
    REVERSION → CHoCH/BOS + divergencia + HTF a favor (2 de 3)
    REBOTE    → RSI extremo sin confirmaciones
    SETUP     → señal normal
    """
    htf_a_favor = (direction == "LONG"  and htf_bias == "BULLISH") or \
                  (direction == "SHORT" and htf_bias == "BEARISH")
    choch_bos   = (direction == "LONG"  and structure in ("CHoCH_BULL", "BOS_BULL")) or \
                  (direction == "SHORT" and structure in ("CHoCH_BEAR", "BOS_BEAR"))
    div_a_favor = (direction == "LONG"  and divergence == "BULLISH_DIV") or \
                  (direction == "SHORT" and divergence == "BEARISH_DIV")
    if sum([choch_bos, div_a_favor, htf_a_favor]) >= 2:
        return "REVERSION"
    rsi_extremo = (direction == "LONG"  and rsi <= (100 - RSI_EXTREME)) or \
                  (direction == "SHORT" and rsi >= RSI_EXTREME)
    if rsi_extremo: return "REBOTE"
    return "SETUP"


def calc_score(direction, rsi, ob, fvg, structure, patterns, vol_high, near_sr, divergence, htf_bias):
    """
    Score de la señal (0-10). Tabla de puntos:
    OB: +2  |  FVG: +1.5  |  Estructura: +1.5  |  RSI extremo: +1.5
    Divergencia: +1.5  |  HTF a favor: +2  |  Vela: +1
    Volumen: +1  |  S/R clave: +1  |  HTF contra: -1
    Fibonacci y HH/LL se suman en analyze_symbol.
    """
    score, labels = 0, []
    rsi_extreme_short = direction == "SHORT" and rsi >= RSI_EXTREME
    rsi_extreme_long  = direction == "LONG"  and rsi <= (100 - RSI_EXTREME)

    if direction == "LONG":
        if rsi <= RSI_OVERSOLD:                    score += 1.5; labels.append("RSI sobrevendido (" + str(rsi) + ")")
        if ob:                                     score += 2;   labels.append("OB alcista " + fmt(ob["low"]) + "-" + fmt(ob["high"]))
        if fvg:                                    score += 1.5; labels.append("FVG alcista " + fmt(fvg["low"]) + "-" + fmt(fvg["high"]))
        if structure in ("BOS_BULL","CHoCH_BULL"): score += 1.5; labels.append("Estructura: " + structure)
        bull_c = [p for p in patterns if "alcista" in p or "Doji" in p]
        if bull_c:                                 score += 1;   labels.append(" | ".join(bull_c))
        if divergence == "BULLISH_DIV":            score += 1.5; labels.append("Divergencia RSI alcista")
        if htf_bias == "BULLISH":                  score += 2;   labels.append("HTF a favor (ALCISTA)")
        elif htf_bias == "BEARISH":                score -= 1
    else:
        if rsi >= RSI_OVERBOUGHT:                  score += 1.5; labels.append("RSI sobrecomprado (" + str(rsi) + ")")
        if ob:                                     score += 2;   labels.append("OB bajista " + fmt(ob["low"]) + "-" + fmt(ob["high"]))
        if fvg:                                    score += 1.5; labels.append("FVG bajista " + fmt(fvg["low"]) + "-" + fmt(fvg["high"]))
        if structure in ("BOS_BEAR","CHoCH_BEAR"): score += 1.5; labels.append("Estructura: " + structure)
        bear_c = [p for p in patterns if "bajista" in p or "Doji" in p]
        if bear_c:                                 score += 1;   labels.append(" | ".join(bear_c))
        if divergence == "BEARISH_DIV":            score += 1.5; labels.append("Divergencia RSI bajista")
        if htf_bias == "BEARISH":                  score += 2;   labels.append("HTF a favor (BAJISTA)")
        elif htf_bias == "BULLISH":                score -= 1

    if vol_high: score += 1; labels.append("Volumen elevado")
    if near_sr:  score += 1; labels.append("Precio en S/R clave")

    if rsi_extreme_short or rsi_extreme_long:
        score = max(score, MIN_SCORE)
        if not any("extremo" in l for l in labels):
            labels.insert(0, "RSI extremo (" + str(rsi) + ") — alerta directa")

    return round(min(max(score, 0), 10)), labels


def calc_sl_tp(price, direction, support, resistance, df=None):
    """
    SL basado en el swing high/low más relevante.

    Para SHORT: SL por encima del swing high más alto de las últimas 50 velas
                que esté por encima del precio de entrada + 0.2% buffer.
    Para LONG:  SL por debajo del swing low más bajo de las últimas 50 velas
                que esté por debajo del precio de entrada - 0.2% buffer.

    Si el swing está muy lejos (>4%) usa el S/R estático como fallback.
    TPs: ratios 1:1.5, 1:2.5, 1:4.0
    """
    sl = None

    if df is not None:
        try:
            if direction == "SHORT":
                # Buscar swing highs en las últimas 50 velas
                recent = df.iloc[-50:]
                swing_highs = []
                for i in range(2, len(recent)-2):
                    if (recent["high"].iloc[i] > recent["high"].iloc[i-1] and
                        recent["high"].iloc[i] > recent["high"].iloc[i+1] and
                        recent["high"].iloc[i] > recent["high"].iloc[i-2] and
                        recent["high"].iloc[i] > recent["high"].iloc[i+2]):
                        swing_highs.append(recent["high"].iloc[i])

                # Tomar el swing high más alto que esté por encima del precio
                highs_sobre_precio = [h for h in swing_highs if h > price]
                if highs_sobre_precio:
                    swing_h = max(highs_sobre_precio)
                    sl_candidato = round(swing_h * 1.002, 6)
                    if (sl_candidato - price) / price <= 0.04:
                        sl = sl_candidato

            else:  # LONG
                # Buscar swing lows en las últimas 50 velas
                recent = df.iloc[-50:]
                swing_lows = []
                for i in range(2, len(recent)-2):
                    if (recent["low"].iloc[i] < recent["low"].iloc[i-1] and
                        recent["low"].iloc[i] < recent["low"].iloc[i+1] and
                        recent["low"].iloc[i] < recent["low"].iloc[i-2] and
                        recent["low"].iloc[i] < recent["low"].iloc[i+2]):
                        swing_lows.append(recent["low"].iloc[i])

                # Tomar el swing low más bajo que esté por debajo del precio
                lows_bajo_precio = [l for l in swing_lows if l < price]
                if lows_bajo_precio:
                    swing_l = min(lows_bajo_precio)
                    sl_candidato = round(swing_l * 0.998, 6)
                    if (price - sl_candidato) / price <= 0.04:
                        sl = sl_candidato
        except:
            sl = None

    # Fallback — usar el máximo/mínimo de las últimas 10 velas + buffer
    # Más inteligente que el S/R estático cuando no hay swing válido
    if sl is None:
        if df is not None:
            try:
                if direction == "LONG":
                    sl = round(df["low"].iloc[-10:].min() * 0.997, 6)
                else:
                    sl = round(df["high"].iloc[-10:].max() * 1.003, 6)
            except:
                sl = None

    # Último fallback al S/R estático
    if sl is None:
        if direction == "LONG":
            sl = round(support * 0.997, 6)
        else:
            sl = round(resistance * 1.003, 6)

    # Calcular riesgo y TPs
    if direction == "LONG":
        risk = max(price - sl, price * 0.005)
        tp1, tp2, tp3 = round(price+risk*1.5, 6), round(price+risk*2.5, 6), round(price+risk*4.0, 6)
    else:
        risk = max(sl - price, price * 0.005)
        tp1, tp2, tp3 = round(price-risk*1.5, 6), round(price-risk*2.5, 6), round(price-risk*4.0, 6)

    return sl, tp1, tp2, tp3, round(abs(tp1-price)/risk, 1), round(abs(tp2-price)/risk, 1)

# =============================================================================
#   FORMATO DE MENSAJES TELEGRAM
# =============================================================================

def format_setup(s, tf_label):
    """Arma el mensaje de Telegram para un setup"""
    d         = s["direction"]
    is_long   = d == "LONG"
    htf_label = "ALCISTA" if s["htf_bias"] == "BULLISH" else ("BAJISTA" if s["htf_bias"] == "BEARISH" else "NEUTRAL")
    htf_emoji = "🟢" if s["htf_bias"] == "BULLISH" else ("🔴" if s["htf_bias"] == "BEARISH" else "⚪")
    stars     = "⭐" * min(int(s["score"]), 5)
    tipo      = s.get("tipo_setup", "SETUP")

    if tipo == "REVERSION": tipo_tag = "🔄 REVERSIÓN"
    elif tipo == "REBOTE":  tipo_tag = "↩️ REBOTE"
    else:                   tipo_tag = ""

    header = ("🟢 <b>LONG 📈 — " if is_long else "🔴 <b>SHORT 📉 — ") + s["symbol"] + "</b>"

    lines = [
        "",
        header + " [" + tf_label + "]" + (" | " + tipo_tag if tipo_tag else ""),
        "Score: <b>" + str(s["score"]) + "/10</b> " + stars,
    ]

    if tipo == "REBOTE":
        lines.append("⚠️ <b>REBOTE TÉCNICO — HTF en contra. TP corto, riesgo alto.</b>")

    lines += [
        "💰 Precio actual: " + fmt(s["price"]),
        "📊 RSI:           " + str(s["rsi"]),
        htf_emoji + " HTF Bias:      " + htf_label,
        "🌊 Volatilidad:   " + str(s["volatility"]) + "%",
        "📊 Volumen:       " + str(s["vol_ratio"]) + "% del promedio",
    ]

    if s["divergence"]:
        div_emoji = "📈" if s["divergence"] == "BULLISH_DIV" else "📉"
        lines.append(div_emoji + " Divergencia:   " + ("Alcista" if s["divergence"] == "BULLISH_DIV" else "Bajista"))
    if s["ob"]:
        ob_emoji = "🟩" if is_long else "🟥"
        lines.append(ob_emoji + " Order Block:   " + fmt(s["ob"]["low"]) + " - " + fmt(s["ob"]["high"]))
    if s["fvg"]:
        sz = s["fvg"].get("size", "")
        lines.append("⬜ FVG:          " + fmt(s["fvg"]["low"]) + " - " + fmt(s["fvg"]["high"]) + (" (" + str(sz) + "%)" if sz else ""))
    if s["structure"]:
        lines.append("🔷 Estructura:   " + s["structure"])

    hh_ll_type  = s.get("hh_ll_type")
    hh_ll_level = s.get("hh_ll_level")
    if hh_ll_type == "HH" and hh_ll_level:
        lines.append("🔺 HH detectado: " + fmt(hh_ll_level) + " — zona de oferta")
    elif hh_ll_type == "LL" and hh_ll_level:
        lines.append("🔻 LL detectado: " + fmt(hh_ll_level) + " — zona de demanda")

    fib_nivel = s.get("fib_nivel")
    fib_desc  = s.get("fib_desc")
    if fib_nivel and fib_desc:
        lines.append("🌀 " + fib_desc)

    lines.append("🕯 Vela:          " + (" | ".join(s["candles"]) if s["candles"] else "Sin patron"))
    lines += [
        "🟢 Soporte:       " + fmt(s["support"]),
        "🔴 Resistencia:   " + fmt(s["resistance"]),
        "━━━━━━━━━━━━━━━",
    ]

    # Entrada estimada + SL + TPs juntos
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
        "🛑 SL:       " + fmt(s["sl"]),
        "🎯 TP1:      " + fmt(s["tp1"]) + " (R:R 1:" + str(s["rr1"]) + ")",
        "🎯 TP2:      " + fmt(s["tp2"]) + " (R:R 1:" + str(s["rr2"]) + ")",
        "🎯 TP3:      " + fmt(s["tp3"]),
        "✅ Confluencias:",
    ]
    for l in s["labels"]:
        lines.append("   • " + l)

    return "\n".join(lines)

# =============================================================================
#   ANÁLISIS POR SÍMBOLO
# =============================================================================

def analyze_symbol(symbol, interval):
    """
    Analiza un par y retorna lista de setups válidos.
    Aplica todos los filtros, calcula score, SL/TP y tipo de setup.
    """
    df = get_klines(symbol, interval, 200)
    if df is None: return None
    try:
        # ── Calcular todos los indicadores ──────────────────────────────────
        price                    = df["close"].iloc[-1]
        rsi                      = calc_rsi(df["close"])
        sup, res                 = calc_sr(df)
        ob_b, ob_s               = detect_ob(df)
        fv_b, fv_s               = detect_fvg(df)
        structure                = detect_structure(df)
        candles                  = detect_candle(df)
        vol_r, vol_h             = calc_vol(df)
        vol                      = calc_volatility(df)
        divergence               = detect_rsi_divergence(df)
        htf_bias                 = get_htf_bias(symbol, interval)
        bias_1h                  = get_1h_bias(symbol) if interval == "15m" else None
        btc_momentum             = get_btc_momentum(interval) if symbol != "BTCUSDT" else "NEUTRAL"
        fib_nivel, fib_desc, fib_dir = detect_fibonacci(df)
        hh_ll_type, hh_ll_level  = detect_hh_ll(df)
        near_sup                 = abs(price - sup) / price < 0.01
        near_res                 = abs(price - res) / price < 0.01

        # ── Guardar estado en DB ─────────────────────────────────────────────
        guardar_estado(symbol, interval, rsi, structure, htf_bias, divergence, price)
        guardar_rsi(symbol, interval, rsi, price)

        results = []
        for direction, ob, fvg, near in [("LONG", ob_b, fv_b, near_sup), ("SHORT", ob_s, fv_s, near_res)]:

            # ── FILTROS DE ENTRADA ───────────────────────────────────────────

            # 1. RSI direccional
            if direction == "SHORT" and rsi < 50 and rsi < RSI_EXTREME: continue
            if direction == "LONG"  and rsi > 50 and rsi > (100-RSI_EXTREME): continue

            # 2. HTF 4H — no operar contra el bias mayor (salvo RSI extremo)
            rsi_extremo = (direction == "SHORT" and rsi >= RSI_EXTREME) or \
                          (direction == "LONG"  and rsi <= (100-RSI_EXTREME))
            if direction == "SHORT" and htf_bias == "BULLISH" and not rsi_extremo: continue
            if direction == "LONG"  and htf_bias == "BEARISH" and not rsi_extremo: continue

            # 3. Filtro 1H intermedio (solo para trades de 15m)
            if bias_1h is not None and not rsi_extremo:
                if direction == "SHORT" and bias_1h == "BULLISH": continue
                if direction == "LONG"  and bias_1h == "BEARISH": continue

            # 4. Correlación BTC — no operar contra el impulso de BTC
            if not rsi_extremo:
                if direction == "SHORT" and btc_momentum == "BULLISH": continue
                if direction == "LONG"  and btc_momentum == "BEARISH": continue

            # 5. Score y clasificación
            score, labels = calc_score(direction, rsi, ob, fvg, structure, candles, vol_h, near, divergence, htf_bias)
            tipo_setup    = clasificar_setup(direction, rsi, structure, divergence, htf_bias)
            if tipo_setup == "REBOTE": score = min(score, 5)

            # Sumar Fibonacci al score si coincide con la dirección
            fib_valido = fib_nivel is not None and fib_dir == direction
            if fib_valido:
                if fib_nivel == 0.886:   score = min(score + 2.5, 10); labels.append(fib_desc)
                elif fib_nivel == 0.618: score = min(score + 2.0, 10); labels.append(fib_desc)
                elif fib_nivel == 0.786: score = min(score + 1.5, 10); labels.append(fib_desc)
                else:                    score = min(score + 1.0, 10); labels.append(fib_desc)

            # 6. Vela contradictoria
            if direction == "LONG"  and any("bajista" in p for p in candles): continue
            if direction == "SHORT" and any("alcista" in p for p in candles): continue

            # 7. Divergencia contraria
            if direction == "SHORT" and divergence == "BULLISH_DIV": continue
            if direction == "LONG"  and divergence == "BEARISH_DIV": continue

            # 8. Estructura contraria
            if direction == "LONG"  and structure in ("BOS_BEAR", "CHoCH_BEAR"): continue
            if direction == "SHORT" and structure in ("BOS_BULL", "CHoCH_BULL"): continue

            # 9. OB — precio debe estar dentro o a máx 0.5% de distancia
            if ob:
                if direction == "SHORT" and (ob["low"] - price) / price > 0.005: continue
                if direction == "LONG"  and (price - ob["high"]) / price > 0.005: continue

            # 10. HH/LL — zona de referencia obligatoria si no hay OB ni FVG
            hh_ll_valido = (direction == "SHORT" and hh_ll_type == "HH") or \
                           (direction == "LONG"  and hh_ll_type == "LL")
            if not ob and not fvg and not hh_ll_valido: continue
            if hh_ll_valido:
                score = min(score + 2, 10)
                labels.append(("HH — zona de oferta " if direction == "SHORT" else "LL — zona de demanda ") + fmt(hh_ll_level))

            # 11. No shortear sobre soporte / no longear bajo resistencia
            if direction == "SHORT" and near_sup: continue
            if direction == "LONG"  and near_res: continue

            # 12. Volumen mínimo 20%
            if vol_r < 20: continue

            # 13. Score mínimo
            if score < MIN_SCORE: continue

            # 14. Cooldown — verificar ANTES de agregar al resultado
            if ya_alerte(symbol, direction, interval): continue

            # ── SL / TPs ─────────────────────────────────────────────────────
            sl, tp1, tp2, tp3, rr1, rr2 = calc_sl_tp(price, direction, sup, res, df)

            # Para REBOTE: SL ajustado al 1% y TPs más cortos
            if tipo_setup == "REBOTE":
                risk = price * 0.01
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
                "tp3":         tp3,
                "rr1":         rr1,
                "rr2":         rr2,
                "volatility":  vol,
                "tipo_setup":  tipo_setup,
                "hh_ll_type":  hh_ll_type,
                "hh_ll_level": hh_ll_level,
                "fib_nivel":   fib_nivel if fib_valido else None,
                "fib_desc":    fib_desc  if fib_valido else None,
            })

        return results if results else None
    except Exception as e:
        print("Error " + symbol + ": " + str(e))
        return None

# =============================================================================
#   ESCANEO PRINCIPAL
# =============================================================================

def scan_all():
    """Escanea todos los pares, guarda en DB y envía por Telegram"""
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

    msg  = "🔍 <b>SCANNER v4.3 — " + now + "</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += "📋 Activos: " + str(len(SYMBOLS)) + " x 2 TF | Setups: " + str(len(all_setups)) + "\n"
    msg += "⚙️ Score mín: " + str(MIN_SCORE) + "/10 | Cooldown: " + str(ALERTA_COOLDOWN_MIN) + "min\n"
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
    print("[" + now + "] Alerta enviada — " + str(len(all_setups)) + " setups")

# =============================================================================
#   RESUMEN DIARIO
# =============================================================================

def run_backtest():
    """
    Backtesting de 1 mes sobre todos los pares y timeframes.
    Simula las señales del bot sobre datos históricos de Binance
    y calcula: win rate, profit/loss, mejor/peor operación, LONG vs SHORT.

    Lógica de simulación:
    - Por cada señal detectada, verifica si el precio alcanzó TP1, TP2, TP3 o SL
    - Usa las siguientes 50 velas después de la señal para simular el resultado
    - Resultado: WIN (llegó a TP1+), LOSS (tocó SL), NEUTRAL (no llegó a ninguno)
    """
    send_telegram("⏳ <b>Iniciando backtesting...</b>\nAnalizando 1 mes de datos para " + str(len(SYMBOLS)) + " pares x 2 TF. Puede tardar unos minutos.")
    print("Iniciando backtesting...")

    resultados = []
    LOOKBACK_VELAS = {
        "15m": 2880,   # 1 mes en velas de 15m = 30d * 24h * 4 = 2880
        "1h":  720,    # 1 mes en velas de 1h  = 30d * 24h = 720
    }
    FUTURE_VELAS = 50  # Velas hacia adelante para verificar resultado

    for interval, tf_label in INTERVALS:
        limit = LOOKBACK_VELAS[interval] + 300  # +300 para indicadores
        for symbol in SYMBOLS:
            try:
                df_full = get_klines(symbol, interval, limit=min(limit, 1000))
                if df_full is None or len(df_full) < 300:
                    print("Sin datos: " + symbol + " " + interval)
                    continue

                # Iterar sobre el período histórico dejando 50 velas al final para simular
                step = 4 if interval == "15m" else 1  # saltar velas para no tardar demasiado
                for i in range(250, len(df_full) - FUTURE_VELAS, step):
                    df_slice = df_full.iloc[:i].copy().reset_index(drop=True)
                    if len(df_slice) < 50: continue

                    try:
                        price        = df_slice["close"].iloc[-1]
                        rsi          = calc_rsi(df_slice["close"])
                        sup, res     = calc_sr(df_slice)
                        ob_b, ob_s   = detect_ob(df_slice)
                        fv_b, fv_s   = detect_fvg(df_slice)
                        structure    = detect_structure(df_slice)
                        candles      = detect_candle(df_slice)
                        vol_r, vol_h = calc_vol(df_slice)
                        divergence   = detect_rsi_divergence(df_slice)
                        htf_bias     = get_htf_bias(symbol, interval)
                        bias_1h      = get_1h_bias(symbol) if interval == "15m" else None
                        btc_momentum = get_btc_momentum(interval) if symbol != "BTCUSDT" else "NEUTRAL"
                        fib_nivel, fib_desc, fib_dir = detect_fibonacci(df_slice)
                        hh_ll_type, hh_ll_level = detect_hh_ll(df_slice)
                        near_sup     = abs(price - sup) / price < 0.01
                        near_res     = abs(price - res) / price < 0.01

                        for direction, ob, fvg, near in [("LONG", ob_b, fv_b, near_sup), ("SHORT", ob_s, fv_s, near_res)]:
                            if direction == "SHORT" and rsi < 50 and rsi < RSI_EXTREME: continue
                            if direction == "LONG"  and rsi > 50 and rsi > (100-RSI_EXTREME): continue
                            rsi_extremo = (direction == "SHORT" and rsi >= RSI_EXTREME) or \
                                          (direction == "LONG"  and rsi <= (100-RSI_EXTREME))
                            if direction == "SHORT" and htf_bias == "BULLISH" and not rsi_extremo: continue
                            if direction == "LONG"  and htf_bias == "BEARISH" and not rsi_extremo: continue
                            # Filtro 1H para trades de 15m
                            if bias_1h is not None and not rsi_extremo:
                                if direction == "SHORT" and bias_1h == "BULLISH": continue
                                if direction == "LONG"  and bias_1h == "BEARISH": continue
                            # Filtro correlación BTC
                            if not rsi_extremo:
                                if direction == "SHORT" and btc_momentum == "BULLISH": continue
                                if direction == "LONG"  and btc_momentum == "BEARISH": continue

                            score, labels = calc_score(direction, rsi, ob, fvg, structure, candles, vol_h, near, divergence, htf_bias)
                            tipo_setup    = clasificar_setup(direction, rsi, structure, divergence, htf_bias)
                            if tipo_setup == "REBOTE": score = min(score, 5)

                            fib_valido = fib_nivel is not None and fib_dir == direction
                            if fib_valido:
                                if fib_nivel == 0.886:   score = min(score + 2.5, 10)
                                elif fib_nivel == 0.618: score = min(score + 2.0, 10)
                                elif fib_nivel == 0.786: score = min(score + 1.5, 10)
                                else:                    score = min(score + 1.0, 10)

                            if direction == "LONG"  and any("bajista" in p for p in candles): continue
                            if direction == "SHORT" and any("alcista" in p for p in candles): continue
                            if direction == "SHORT" and divergence == "BULLISH_DIV": continue
                            if direction == "LONG"  and divergence == "BEARISH_DIV": continue
                            if direction == "LONG"  and structure in ("BOS_BEAR", "CHoCH_BEAR"): continue
                            if direction == "SHORT" and structure in ("BOS_BULL", "CHoCH_BULL"): continue
                            if ob:
                                if direction == "SHORT" and (ob["low"] - price) / price > 0.005: continue
                                if direction == "LONG"  and (price - ob["high"]) / price > 0.005: continue
                            hh_ll_valido = (direction == "SHORT" and hh_ll_type == "HH") or \
                                           (direction == "LONG"  and hh_ll_type == "LL")
                            if not ob and not fvg and not hh_ll_valido: continue
                            if hh_ll_valido: score = min(score + 2, 10)
                            if direction == "SHORT" and near_sup: continue
                            if direction == "LONG"  and near_res: continue
                            if vol_r < 20: continue
                            if score < MIN_SCORE: continue

                            # Calcular SL y TPs
                            sl, tp1, tp2, tp3, rr1, rr2 = calc_sl_tp(price, direction, sup, res, df_slice)
                            if tipo_setup == "REBOTE":
                                risk = price * 0.01
                                if direction == "LONG":
                                    sl = round(price - risk, 6); tp1 = round(price + risk * 0.8, 6)
                                    tp2 = round(price + risk * 1.5, 6); tp3 = round(price + risk * 2.0, 6)
                                else:
                                    sl = round(price + risk, 6); tp1 = round(price - risk * 0.8, 6)
                                    tp2 = round(price - risk * 1.5, 6); tp3 = round(price - risk * 2.0, 6)

                            # Simular resultado con las siguientes 50 velas
                            future = df_full.iloc[i:i+FUTURE_VELAS]
                            resultado = "NEUTRAL"
                            tp_alcanzado = 0
                            pnl_pct = 0.0

                            for _, vela in future.iterrows():
                                h, l = vela["high"], vela["low"]
                                if direction == "LONG":
                                    if l <= sl:
                                        resultado = "LOSS"
                                        pnl_pct = round((sl - price) / price * 100, 2)
                                        break
                                    elif h >= tp3:
                                        resultado = "WIN"; tp_alcanzado = 3
                                        pnl_pct = round((tp3 - price) / price * 100, 2); break
                                    elif h >= tp2:
                                        resultado = "WIN"; tp_alcanzado = 2
                                        pnl_pct = round((tp2 - price) / price * 100, 2)
                                    elif h >= tp1 and tp_alcanzado == 0:
                                        resultado = "WIN"; tp_alcanzado = 1
                                        pnl_pct = round((tp1 - price) / price * 100, 2)
                                else:
                                    if h >= sl:
                                        resultado = "LOSS"
                                        pnl_pct = round((price - sl) / price * 100, 2)
                                        break
                                    elif l <= tp3:
                                        resultado = "WIN"; tp_alcanzado = 3
                                        pnl_pct = round((price - tp3) / price * 100, 2); break
                                    elif l <= tp2:
                                        resultado = "WIN"; tp_alcanzado = 2
                                        pnl_pct = round((price - tp2) / price * 100, 2)
                                    elif l <= tp1 and tp_alcanzado == 0:
                                        resultado = "WIN"; tp_alcanzado = 1
                                        pnl_pct = round((price - tp1) / price * 100, 2)

                            if resultado != "NEUTRAL":
                                resultados.append({
                                    "symbol":    symbol,
                                    "direction": direction,
                                    "timeframe": tf_label,
                                    "score":     score,
                                    "tipo":      tipo_setup,
                                    "resultado": resultado,
                                    "tp":        tp_alcanzado,
                                    "pnl_pct":   pnl_pct if resultado == "WIN" else -abs(pnl_pct),
                                    "precio":    price,
                                })
                    except:
                        continue
                time.sleep(0.1)
                print("BT " + symbol + " " + interval + " — señales: " + str(len([r for r in resultados if r["symbol"] == symbol])))
            except Exception as e:
                print("Error BT " + symbol + ": " + str(e))

    # ── Calcular métricas ────────────────────────────────────────────────────
    if not resultados:
        send_telegram("❌ Backtesting sin resultados — no se encontraron señales en el período.")
        return

    total    = len(resultados)
    wins     = [r for r in resultados if r["resultado"] == "WIN"]
    losses   = [r for r in resultados if r["resultado"] == "LOSS"]
    win_rate = round(len(wins) / total * 100, 1)
    pnl_avg  = round(sum(r["pnl_pct"] for r in resultados) / total, 2)
    pnl_wins = round(sum(r["pnl_pct"] for r in wins) / len(wins), 2) if wins else 0
    pnl_loss = round(sum(r["pnl_pct"] for r in losses) / len(losses), 2) if losses else 0

    mejor = max(resultados, key=lambda x: x["pnl_pct"])
    peor  = min(resultados, key=lambda x: x["pnl_pct"])

    longs  = [r for r in resultados if r["direction"] == "LONG"]
    shorts = [r for r in resultados if r["direction"] == "SHORT"]
    wr_long  = round(len([r for r in longs  if r["resultado"] == "WIN"]) / len(longs)  * 100, 1) if longs  else 0
    wr_short = round(len([r for r in shorts if r["resultado"] == "WIN"]) / len(shorts) * 100, 1) if shorts else 0

    # TPs alcanzados
    tp1_count = len([r for r in wins if r["tp"] >= 1])
    tp2_count = len([r for r in wins if r["tp"] >= 2])
    tp3_count = len([r for r in wins if r["tp"] == 3])

    # Por tipo de setup
    rev  = [r for r in resultados if r["tipo"] == "REVERSION"]
    reb  = [r for r in resultados if r["tipo"] == "REBOTE"]
    stp  = [r for r in resultados if r["tipo"] == "SETUP"]
    wr_rev = round(len([r for r in rev if r["resultado"] == "WIN"]) / len(rev)  * 100, 1) if rev  else 0
    wr_reb = round(len([r for r in reb if r["resultado"] == "WIN"]) / len(reb)  * 100, 1) if reb  else 0
    wr_stp = round(len([r for r in stp if r["resultado"] == "WIN"]) / len(stp)  * 100, 1) if stp  else 0

    # ── Armar mensaje ────────────────────────────────────────────────────────
    msg  = "📊 <b>BACKTESTING — 1 MES</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += "📋 Total señales analizadas: <b>" + str(total) + "</b>\n"
    msg += "✅ Ganadoras: <b>" + str(len(wins)) + "</b>  ❌ Perdedoras: <b>" + str(len(losses)) + "</b>\n\n"

    msg += "🏆 <b>WIN RATE GENERAL</b>\n"
    msg += "Win Rate: <b>" + str(win_rate) + "%</b>\n"
    msg += "P&L promedio: <b>" + str(pnl_avg) + "%</b>\n"
    msg += "Ganancia prom WIN: <b>+" + str(pnl_wins) + "%</b>\n"
    msg += "Pérdida prom LOSS: <b>" + str(pnl_loss) + "%</b>\n\n"

    msg += "🎯 <b>TPs ALCANZADOS</b>\n"
    msg += "TP1: " + str(tp1_count) + " veces  TP2: " + str(tp2_count) + "  TP3: " + str(tp3_count) + "\n\n"

    msg += "📈 <b>LONG vs SHORT</b>\n"
    msg += "LONG:  " + str(len(longs))  + " señales — Win rate: <b>" + str(wr_long)  + "%</b>\n"
    msg += "SHORT: " + str(len(shorts)) + " señales — Win rate: <b>" + str(wr_short) + "%</b>\n\n"

    msg += "🔄 <b>POR TIPO DE SETUP</b>\n"
    msg += "REVERSIÓN: " + str(len(rev)) + " señales — Win rate: <b>" + str(wr_rev) + "%</b>\n"
    msg += "REBOTE:    " + str(len(reb)) + " señales — Win rate: <b>" + str(wr_reb) + "%</b>\n"
    msg += "SETUP:     " + str(len(stp)) + " señales — Win rate: <b>" + str(wr_stp) + "%</b>\n\n"

    msg += "🥇 <b>MEJOR OPERACIÓN</b>\n"
    msg += mejor["direction"] + " " + mejor["symbol"] + " [" + mejor["timeframe"] + "]\n"
    msg += "Score: " + str(mejor["score"]) + " | TP" + str(mejor["tp"]) + " | +" + str(mejor["pnl_pct"]) + "%\n\n"

    msg += "💀 <b>PEOR OPERACIÓN</b>\n"
    msg += peor["direction"] + " " + peor["symbol"] + " [" + peor["timeframe"] + "]\n"
    msg += "Score: " + str(peor["score"]) + " | " + str(peor["pnl_pct"]) + "%\n\n"

    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += "🕐 " + datetime.now(ARG_TZ).strftime("%d/%m %H:%M")

    send_telegram(msg)
    print("Backtesting completado — " + str(total) + " señales")

# =============================================================================
#   RESUMEN DIARIO
# =============================================================================

def resumen_diario():
    """Envía resumen de las últimas 20 alertas — automático a las 8am o con /resumen"""
    historial = get_historial_alertas(20)
    if not historial: return
    msg  = "📅 <b>RESUMEN DIARIO — " + datetime.now(ARG_TZ).strftime("%d/%m/%Y") + "</b>\n\n"
    msg += "Últimas " + str(len(historial)) + " alertas:\n"
    for row in historial:
        symbol, direction, timeframe, precio, score, enviada_at = row
        emoji = "🟢" if direction == "LONG" else "🔴"
        msg += emoji + " " + ("Long" if direction == "LONG" else "Short") + " " + symbol
        msg += " [" + timeframe + "] Score:" + str(score)
        msg += " @ " + fmt(precio)
        msg += " — " + enviada_at.strftime("%d/%m %H:%M") + "\n"
    send_telegram(msg)

# =============================================================================
#   INICIO
# =============================================================================

if __name__ == "__main__":
    print("Bot Scanner Crypto v4.3 iniciado...")
    init_db()
    send_telegram(
        "<b>🤖 Bot Scanner Crypto v4.3 ACTIVO</b>\n\n"
        "✅ Novedades v4.3:\n"
        "— Fibonacci agregado (0.382/0.5/0.618/0.786/0.886)\n"
        "— Correlación BTC como filtro\n"
        "— Filtro 1H intermedio para 15m\n"
        "— Código limpio sin errores\n\n"
        "⚙️ Score mínimo: " + str(MIN_SCORE) + "/10\n"
        "⏱ Cooldown: " + str(ALERTA_COOLDOWN_MIN) + " minutos\n"
        "🔄 Escaneo cada 5 minutos\n\n"
        "💬 Comandos: /resumen | /scan | /backtest | /ayuda"
    )
    scan_all()
    schedule.every(5).minutes.do(scan_all)
    schedule.every(5).minutes.do(verificar_resultados)
    schedule.every().day.at("08:00").do(resumen_diario)
    last_update_id = None
    while True:
        schedule.run_pending()
        last_update_id = procesar_comandos(last_update_id)
        time.sleep(30)
