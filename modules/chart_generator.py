"""
CRYPTO MONITOR PRO — Chart Generator

Coin uchun narx + hajm grafigini PNG (bytes) ko'rinishida generatsiya qiladi.
Signal xabariga rasm sifatida biriktiriladi.
"""
import io
from datetime import datetime, timezone
from loguru import logger

import matplotlib
matplotlib.use("Agg")  # GUI kerak emas, faqat fayl/bytes generatsiya
import matplotlib.pyplot as plt

BINANCE_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"

# Foydalanuvchi tanlay oladigan timeframe'lar: key -> (Binance interval, shamlar soni, tugma matni)
TIMEFRAMES = {
    "1m":  ("1m", 200, "1m"),
    "5m":  ("5m", 200, "5m"),
    "15m": ("15m", 200, "15m"),
    "1h":  ("1h", 200, "1H"),
    "4h":  ("4h", 200, "4H"),
    "1d":  ("1d", 200, "1D"),
}

# Signal xabariga avtomatik biriktiriladigan default grafik — H4, toza (chizmasiz)
DEFAULT_TIMEFRAME = "4h"


async def _fetch_klines(symbol: str, interval: str, limit: int) -> list:
    """Binance Futures'dan kline (candle) ma'lumotlarini olish"""
    try:
        import aiohttp
        url = f"{BINANCE_KLINES_URL}?symbol={symbol}&interval={interval}&limit={limit}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    logger.debug(f"Kline xato {symbol}: HTTP {resp.status}")
    except Exception as e:
        logger.debug(f"Kline fetch xato {symbol}: {e}")
    return []


def _render_chart(symbol: str, times, opens, highs, lows, closes, volumes, title_extra: str = "") -> bytes:
    """Yapon shamlari (OHLC candlestick) grafigini PNG bytes sifatida qaytaradi.
    Hech qanday indikator, hajm paneli, chiziq yoki belgilarsiz — toza, faqat shamlar."""

    BG = "#0d1117"
    FG = "#c9d1d9"
    GRID = "#30363d"
    GREEN = "#26a641"
    RED = "#f85149"

    fig, ax1 = plt.subplots(figsize=(11, 6), facecolor=BG)

    # Shamlar oralig'ini (sham kengligi) hisoblaymiz — vaqt o'qi sana sifatida
    if len(times) > 1:
        step_days = (times[-1] - times[0]).total_seconds() / (len(times) - 1) / 86400
    else:
        step_days = 1 / 1440
    body_width = step_days * 0.7

    for t, o, h, l, c in zip(times, opens, highs, lows, closes):
        color = GREEN if c >= o else RED
        # Wick (yuqori-past chiziq)
        ax1.vlines(t, l, h, color=color, linewidth=1.0, zorder=2)
        # Gavda (ochilish-yopilish)
        body_bottom = min(o, c)
        body_height = abs(c - o) or (h - l) * 0.001 or 0.0000001
        ax1.bar(t, body_height, bottom=body_bottom, width=body_width, color=color, zorder=3)

    ax1.set_facecolor(BG)
    ax1.tick_params(colors=FG, labelsize=8, rotation=0)
    for spine in ax1.spines.values():
        spine.set_color(GRID)
    ax1.grid(alpha=0.25, color=GRID, linewidth=0.5)
    for label in ax1.get_xticklabels():
        label.set_ha("center")

    change_pct = (closes[-1] - opens[0]) / opens[0] * 100 if opens[0] else 0
    sign = "+" if change_pct >= 0 else ""
    title = f"{symbol}  {sign}{change_pct:.2f}%"
    if title_extra:
        title += f"  •  {title_extra}"
    ax1.set_title(title, color=FG, fontsize=12, fontweight="bold", loc="left")

    last_price = closes[-1]
    last_color = GREEN if closes[-1] >= opens[-1] else RED
    if last_price >= 100:
        price_label = f"{last_price:,.2f}"
    elif last_price >= 1:
        price_label = f"{last_price:.4f}"
    elif last_price >= 0.0001:
        price_label = f"{last_price:.6f}"
    else:
        price_label = f"{last_price:.8f}"
    ax1.annotate(
        price_label,
        xy=(times[-1], last_price),
        xytext=(8, 0), textcoords="offset points",
        color=last_color, fontsize=9, fontweight="bold",
        va="center",
    )

    buf = io.BytesIO()
    plt.savefig(buf, format="png", facecolor=fig.get_facecolor(), dpi=120, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


async def generate_price_chart(
    symbol: str,
    timeframe: str = DEFAULT_TIMEFRAME,
    title_extra: str = "",
) -> bytes | None:
    """
    Berilgan timeframe ('1m','5m','15m','1h','4h','1d') uchun
    toza narx+hajm grafigini (hech qanday chiziq/level/marker'siz) generatsiya qiladi.
    """
    interval, limit, _ = TIMEFRAMES.get(timeframe, TIMEFRAMES[DEFAULT_TIMEFRAME])

    klines = await _fetch_klines(symbol, interval, limit)
    if not klines or len(klines) < 2:
        return None

    try:
        times = [datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc) for k in klines]
        opens = [float(k[1]) for k in klines]
        highs = [float(k[2]) for k in klines]
        lows = [float(k[3]) for k in klines]
        closes = [float(k[4]) for k in klines]
        volumes = [float(k[7]) for k in klines]  # quote asset volume (USDT)
        return _render_chart(symbol, times, opens, highs, lows, closes, volumes, title_extra or timeframe.upper())
    except Exception as e:
        logger.debug(f"Chart render xato {symbol}: {e}")
        return None


def build_timeframe_keyboard(symbol: str, exchange: str, active: str = DEFAULT_TIMEFRAME):
    """
    Grafik ostiga qo'yiladigan timeframe almashtirish tugmalari.
    InlineKeyboardButton ro'yxati (bot/telegram_bot.py'da InlineKeyboardMarkup'ga o'raladi).
    """
    row = []
    for key, (_, _, label) in TIMEFRAMES.items():
        text = f"• {label} •" if key == active else label
        row.append({"text": text, "callback_data": f"chart:{exchange}:{symbol}:{key}"})
    return [row]
