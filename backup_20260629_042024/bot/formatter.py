"""
CRYPTO MONITOR PRO — Telegram Formatter v3
- Consolidated signal format (1 message per coin)
- Real liquidation clusters from WebSocket
- Mobile-friendly Bookmap (Price-First vertical)
- CEXTrack-style activity alerts
- Show More / Qisqartirish pattern
- Live PNL tracking display
"""
from datetime import datetime, timedelta
from core.models import (
    MarketAlert, AlertLevel, Direction,
    VolumeEvent, OIEvent, LiquidationEvent,
    WhaleEvent, FundingEvent, OrderBookEvent
)


def fmt_usdt(v: float) -> str:
    if v >= 1_000_000_000:
        return f"{v/1_000_000_000:.2f}B$"
    elif v >= 1_000_000:
        return f"{v/1_000_000:.1f}M$"
    elif v >= 1_000:
        return f"{v/1_000:.0f}K$"
    return f"{v:.0f}$"


def fmt_token(v: float) -> str:
    if v >= 1_000_000_000:
        return f"{v/1_000_000_000:.2f}B"
    elif v >= 1_000_000:
        return f"{v/1_000_000:.2f}M"
    elif v >= 1_000:
        return f"{v/1_000:.2f}K"
    return f"{v:.2f}"


def fmt_price(p: float) -> str:
    if p >= 10000:
        return f"{p:,.0f}"
    elif p >= 100:
        return f"{p:,.2f}"
    elif p >= 1:
        return f"{p:.4f}"
    elif p > 0:
        return f"{p:.6f}"
    return "—"


def fmt_time(dt: datetime, tz_offset: int = 0) -> str:
    if tz_offset:
        dt = dt + timedelta(hours=tz_offset)
    return dt.strftime("%H:%M:%S")


def fmt_dur(s: int) -> str:
    if s < 60:
        return f"{s}s"
    elif s < 3600:
        return f"{s//60}m {s%60}s"
    return f"{s//3600}h {(s%3600)//60}m"


def fmt_pnl(v: float) -> str:
    ico = "🟢" if v > 0 else "🔴" if v < 0 else "⚪"
    return f"{ico} {v:+.2f}%"


def fmt_pnl_abs(v: float) -> str:
    ico = "🟢" if v > 0 else "🔴" if v < 0 else "⚪"
    return f"{ico} {v:+.2f}%"


def _pct_color(v: float) -> str:
    return "🟢" if v > 0 else "🔴" if v < 0 else "⚪"


def get_signal_strength(alert: MarketAlert) -> tuple[str, str]:
    event_count = sum([
        bool(alert.volume_event),
        bool(alert.oi_event),
        bool(alert.liq_event),
        bool(alert.whale_event),
        bool(alert.funding_event),
        bool(alert.orderbook_event),
    ])
    score = alert.total_score

    if event_count >= 4 or score >= 40:
        return "🚨", "EKSTREM"
    elif event_count >= 3 or score >= 20:
        return "🔥", "KUCHLI"
    elif event_count >= 2:
        return "⚡", "SIGNAL"
    else:
        return "📡", "KUZATUV"


# ═══════════════════════════════════════════════════════════════
# CONSOLIDATED SIGNAL — SHORT (Show More oldin)
# ═══════════════════════════════════════════════════════════════

def build_short_signal(
    alert: MarketAlert,
    price_changes: dict = None,
    cvd_data: dict = None,
    tz_offset: int = 0,
) -> str:
    """Short signal text — shown first, 'Show More' reveals full details"""
    extra = getattr(alert, "extra", {}) or {}
    now = datetime.utcnow()
    price = alert.current_price
    price_str = fmt_price(price) if price > 0 else "—"

    lines = []
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"🪙 <b>{alert.symbol}</b> — {price_str}$")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")

    # ── WHALE ────────────────────────────────────────
    w = alert.whale_event
    if w:
        dir_word = "BUY 🟢" if w.direction.value == "buy" else "SELL 🔴"
        token_qty = getattr(w, "token_qty", 0) or 0
        token_str = f" ({fmt_token(token_qty)})" if token_qty > 0 else ""
        lines.append(f"🐋 <b>WHALE — {dir_word}</b>")
        lines.append(f"  {fmt_usdt(w.volume_usdt)}{token_str}")
        vol_24h = extra.get("volume_24h", 0) or 0
        if vol_24h > 0:
            vol_pct = w.volume_usdt / vol_24h * 100
            lines.append(f"  24h: {fmt_usdt(vol_24h)} | {vol_pct:.2f}%")
    elif extra.get("whale_liquidity_usdt", 0) > 0:
        buy_v = extra.get("whale_buy_volume", 0) or 0
        sell_v = extra.get("whale_sell_volume", 0) or 0
        total_w = extra["whale_liquidity_usdt"]
        bias = "BUY 🟢" if buy_v >= sell_v else "SELL 🔴"
        token_qty = extra.get("whale_token_qty", 0) or 0
        token_str = f" ({fmt_token(token_qty)})" if token_qty > 0 else ""
        lines.append(f"🐋 <b>WHALE — {bias}</b>")
        lines.append(f"  {fmt_usdt(total_w)}{token_str}")
        vol_24h = extra.get("volume_24h", 0) or 0
        if vol_24h > 0:
            vol_pct = total_w / vol_24h * 100
            lines.append(f"  24h: {fmt_usdt(vol_24h)} | {vol_pct:.2f}%")
    elif extra.get("last_whale_usdt", 0) > 0:
        lw_usdt = extra["last_whale_usdt"]
        lw_side = extra.get("last_whale_side", "buy")
        lw_ago = extra.get("last_whale_ago", 0)
        lw_qty = extra.get("last_whale_qty", 0)
        dir_word = "BUY 🟢" if lw_side == "buy" else "SELL 🔴"
        token_str = f" ({fmt_token(lw_qty)})" if lw_qty > 0 else ""
        lines.append(f"🐋 <b>WHALE — {dir_word}</b>")
        lines.append(f"  {fmt_usdt(lw_usdt)}{token_str}")
        if lw_ago > 0:
            lines.append(f"  {fmt_dur(lw_ago)} oldin")
    lines.append("")

    # ── OI + FUNDING ─────────────────────────────────
    oi = alert.oi_event
    f_ = alert.funding_event
    oi_f_parts = []
    if oi:
        oi_usdt = fmt_usdt(oi.oi_usdt) if oi.oi_usdt else "—"
        oi_ico = "▲" if oi.change_pct > 0 else "▼"
        oi_f_parts.append(f"📊 OI: {oi_usdt} {oi_ico} {oi.change_pct:+.1f}%")
    if f_:
        fr_ico = "🔴" if f_.funding_rate > 0 else "🟢"
        oi_f_parts.append(f"{fr_ico} Funding: {f_.funding_rate:+.4f}%")
    if oi_f_parts:
        lines.append(" | ".join(oi_f_parts))
        lines.append("")

    # ── ORDERBOOK ────────────────────────────────────
    ob_buy = extra.get("ob_buy_walls", [])
    ob_sell = extra.get("ob_sell_walls", [])
    imbalance = extra.get("ob_imbalance", 1.0)
    buy_price = fmt_price(ob_buy[0]["price"]) if ob_buy else "—"
    buy_usdt = fmt_usdt(ob_buy[0]["usdt"]) if ob_buy else "—"
    sell_price = fmt_price(ob_sell[0]["price"]) if ob_sell else "—"
    sell_usdt = fmt_usdt(ob_sell[0]["usdt"]) if ob_sell else "—"
    ob_bias = "BUY 🟢" if imbalance >= 1 else "SELL 🔴"
    lines.append("📖 <b>ORDERBOOK</b>")
    lines.append(f"  Buy: {buy_price} ({buy_usdt}) | Sell: {sell_price} ({sell_usdt})")
    lines.append(f"  Imbalance: {imbalance:.1f}x {ob_bias}")
    lines.append("")

    # ── VOLUME + CVD ─────────────────────────────────
    c5m = (price_changes.get("change_5m", 0) if price_changes else 0) or extra.get("price_change_5m", 0) or 0
    c1h = (price_changes.get("change_1h", 0) if price_changes else 0) or extra.get("price_change_1h", 0) or 0
    c24h = (price_changes.get("change_24h", 0) if price_changes else 0) or extra.get("price_change_24h", 0) or 0
    vol_5m = extra.get("vol_5m", 0) or 0
    vol_1h = extra.get("vol_1h", 0) or 0
    cvd_5m = (cvd_data.get("cvd_5m", 0) if cvd_data else 0) or extra.get("cvd_5m", 0) or 0
    taker = extra.get("taker_ratio", 0)

    vol_parts = []
    if vol_5m > 0:
        vol_parts.append(f"5m: {fmt_usdt(vol_5m)}")
    if vol_1h > 0:
        vol_parts.append(f"1h: {fmt_usdt(vol_1h)}")

    extra_parts = []
    if abs(cvd_5m) > 0:
        cvd_ico = "🟢" if cvd_5m > 0 else "🔴"
        extra_parts.append(f"CVD: {cvd_ico} {fmt_usdt(abs(cvd_5m))}")
    if taker > 0:
        tb = "BUY 🟢" if taker > 1.1 else "SELL 🔴" if taker < 0.9 else "⚪"
        extra_parts.append(f"Taker: {taker:.2f}x {tb}")
    if c5m != 0:
        extra_parts.append(f"5m {_pct_color(c5m)} {c5m:+.2f}%")
    if c1h != 0:
        extra_parts.append(f"1h {_pct_color(c1h)} {c1h:+.2f}%")
    if c24h != 0:
        extra_parts.append(f"24h {_pct_color(c24h)} {c24h:+.2f}%")

    if vol_parts or extra_parts:
        lines.append("💹 <b>VOLUME</b>")
        if vol_parts:
            lines.append(f"  {' | '.join(vol_parts)}")
        if extra_parts:
            lines.append(f"  {' | '.join(extra_parts)}")
        lines.append("")

    # ── TIME ─────────────────────────────────────────
    time_str = fmt_time(now, tz_offset)
    lines.append(f"⏱ {time_str} UTC")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# CONSOLIDATED SIGNAL — FULL (Show More keying)
# ═══════════════════════════════════════════════════════════════

def build_full_signal(
    alert: MarketAlert,
    price_changes: dict = None,
    cvd_data: dict = None,
    tz_offset: int = 0,
) -> str:
    """Full signal text — shown after 'Show More' button"""
    extra = getattr(alert, "extra", {}) or {}
    now = datetime.utcnow()
    price = alert.current_price
    price_str = fmt_price(price) if price > 0 else "—"
    strength_ico, strength_txt = get_signal_strength(alert)

    lines = []
    lines.append(f"{strength_ico} <b>#{alert.symbol} — {strength_txt}</b>")
    lines.append(f"💵 <b>{price_str}$</b>")
    lines.append("")

    # ── WHALE ────────────────────────────────────────
    w = alert.whale_event
    buy_v = extra.get("whale_buy_volume", 0) or 0
    sell_v = extra.get("whale_sell_volume", 0) or 0
    token_qty = 0
    vol_pct = 0
    vol_24h = extra.get("volume_24h", 0) or 0

    if w:
        token_qty = getattr(w, "token_qty", 0) or 0
        vol_pct = getattr(w, "volume_pct_of_24h", 0)
        vol_24h = vol_24h or getattr(w, "volume_24h", 0) or 0
        vol_pct = vol_pct or (w.volume_usdt / vol_24h * 100) if vol_24h > 0 else 0
        dir_word = "BUY 🟢" if w.direction.value == "buy" else "SELL 🔴"
        token_str = f" ({fmt_token(token_qty)})" if token_qty > 0 else ""
        lines.append(f"🐋 <b>WHALE — {dir_word}</b>")
        lines.append(f"  {fmt_usdt(w.volume_usdt)}{token_str}")
        lines.append(f"  {fmt_dur(w.duration_seconds)} | {w.order_count} ta trade")
        lines.append(f"  24h: {fmt_usdt(vol_24h)} | {vol_pct:.2f}%")
    elif extra.get("whale_liquidity_usdt", 0) > 0:
        total_w = extra["whale_liquidity_usdt"]
        token_qty = extra.get("whale_token_qty", 0) or 0
        vol_pct = extra.get("whale_pct_24h", 0) or (total_w / vol_24h * 100) if vol_24h > 0 else 0
        bias = "BUY 🟢" if buy_v >= sell_v else "SELL 🔴"
        token_str = f" ({fmt_token(token_qty)})" if token_qty > 0 else ""
        lines.append(f"🐋 <b>WHALE — {bias}</b>")
        lines.append(f"  {fmt_usdt(total_w)}{token_str}")
        if vol_pct > 0:
            lines.append(f"  24h: {fmt_usdt(vol_24h)} | {vol_pct:.2f}%")
    elif extra.get("last_whale_usdt", 0) > 0:
        lw_usdt = extra["last_whale_usdt"]
        lw_side = extra.get("last_whale_side", "buy")
        lw_ago = extra.get("last_whale_ago", 0)
        lw_qty = extra.get("last_whale_qty", 0)
        dir_word = "BUY 🟢" if lw_side == "buy" else "SELL 🔴"
        token_str = f" ({fmt_token(lw_qty)})" if lw_qty > 0 else ""
        lines.append(f"🐋 <b>WHALE — {dir_word}</b>")
        lines.append(f"  {fmt_usdt(lw_usdt)}{token_str}")
        if lw_ago > 0:
            lines.append(f"  {fmt_dur(lw_ago)} oldin")
    lines.append("")

    # ── OI + FUNDING ─────────────────────────────────
    oi = alert.oi_event
    f_ = alert.funding_event
    if oi or f_:
        parts = []
        if oi:
            oi_ico = "▲" if oi.change_pct > 0 else "▼"
            parts.append(f"📊 <b>OI:</b> {fmt_usdt(oi.oi_usdt)} {oi_ico} {oi.change_pct:+.1f}%")
        if f_:
            fund_ico = "🔴" if f_.funding_rate > 0 else "🟢"
            parts.append(f"{fund_ico} <b>Funding:</b> {f_.funding_rate:+.4f}%")
        lines.append(" | ".join(parts))
        lines.append("")

    # ── LIQUIDATIONS (real from aggregator) ────────────
    clusters = getattr(alert, "liq_clusters", []) or []
    liq = alert.liq_event
    liq_total = extra.get("liq_real_total", 0)
    liq_long = extra.get("liq_real_long", 0) or (liq.long_liq_usdt if liq else 0)
    liq_short = extra.get("liq_real_short", 0) or (liq.short_liq_usdt if liq else 0)

    if clusters:
        lines.append("💥 <b>LIKVIDATSIYA ZONALARI</b> <i>(real)</i>")
        for c in clusters[:5]:
            side_ico = "🔴" if c["side"] == "long_liq" else "🟢"
            lines.append(f"  {side_ico} {fmt_price(c['price'])}$ — {fmt_usdt(c['total_usdt'])} ({c['count']}x)")
        lines.append("")
    elif liq_total > 0 or liq:
        if liq_total == 0 and liq:
            liq_total = liq.long_liq_usdt + liq.short_liq_usdt
        dom = "🟥 SHORT" if liq_short > liq_long else "🟩 LONG" if liq_long > liq_short else "⚪ MIXED"
        lines.append(f"💥 <b>LIQUIDATIONS</b>")
        lines.append(f"  LONG: {fmt_usdt(liq_long)} | SHORT: {fmt_usdt(liq_short)}")
        lines.append(f"  {dom} dominance")
        lines.append("")

    # ── ORDERBOOK ────────────────────────────────────
    ob_buy = extra.get("ob_buy_walls", [])
    ob_sell = extra.get("ob_sell_walls", [])
    imbalance = extra.get("ob_imbalance", 1.0)
    if ob_buy or ob_sell:
        buy_price = fmt_price(ob_buy[0]["price"]) if ob_buy else "—"
        buy_usdt = fmt_usdt(ob_buy[0]["usdt"]) if ob_buy else "—"
        sell_price = fmt_price(ob_sell[0]["price"]) if ob_sell else "—"
        sell_usdt = fmt_usdt(ob_sell[0]["usdt"]) if ob_sell else "—"
        bias = "BUY 🟢" if imbalance >= 1 else "SELL 🔴"
        lines.append("📖 <b>ORDERBOOK</b>")
        lines.append(f"  Buy: {buy_price} ({buy_usdt}) | Sell: {sell_price} ({sell_usdt})")
        lines.append(f"  Imbalance: {imbalance:.1f}x {bias}")
        lines.append("")

    # ── VOLUME + CVD + TAKER ─────────────────────────
    c1m = (price_changes.get("change_1m", 0) if price_changes else 0) or extra.get("price_change_1m", 0) or 0
    c5m = (price_changes.get("change_5m", 0) if price_changes else 0) or extra.get("price_change_5m", 0) or 0
    c1h = (price_changes.get("change_1h", 0) if price_changes else 0) or extra.get("price_change_1h", 0) or 0
    c24h = (price_changes.get("change_24h", 0) if price_changes else 0) or extra.get("price_change_24h", 0) or 0
    vol_5m = extra.get("vol_5m", 0) or 0
    vol_1h = extra.get("vol_1h", 0) or 0
    cvd_5m = (cvd_data.get("cvd_5m", 0) if cvd_data else 0) or extra.get("cvd_5m", 0) or 0
    taker = extra.get("taker_ratio", 0)

    vol_parts = []
    if vol_5m > 0:
        vol_parts.append(f"5m: {fmt_usdt(vol_5m)}")
    if vol_1h > 0:
        vol_parts.append(f"1h: {fmt_usdt(vol_1h)}")

    extra_parts = []
    if abs(cvd_5m) > 0:
        cvd_ico = "🟢" if cvd_5m > 0 else "🔴"
        extra_parts.append(f"CVD: {cvd_ico} {fmt_usdt(abs(cvd_5m))}")
    if taker > 0:
        tb = "BUY 🟢" if taker > 1.1 else "SELL 🔴" if taker < 0.9 else "⚪"
        extra_parts.append(f"Taker: {taker:.2f}x {tb}")
    if c5m != 0:
        extra_parts.append(f"5m {_pct_color(c5m)} {c5m:+.2f}%")
    if c1h != 0:
        extra_parts.append(f"1h {_pct_color(c1h)} {c1h:+.2f}%")
    if c24h != 0:
        extra_parts.append(f"24h {_pct_color(c24h)} {c24h:+.2f}%")

    if vol_parts or extra_parts:
        lines.append("💹 <b>VOLUME + CVD</b>")
        if vol_parts:
            lines.append(f"  {' | '.join(vol_parts)}")
        if extra_parts:
            lines.append(f"  {' | '.join(extra_parts)}")
        lines.append("")

    # ── STRATEGY ─────────────────────────────────────
    if alert.strategy_direction and alert.strategy_entry > 0:
        entry = alert.strategy_entry
        sl = alert.strategy_sl
        tp = alert.strategy_tp
        direction = alert.strategy_direction

        if direction == "LONG":
            sl_pct = ((sl - entry) / entry * 100) if entry > 0 else 0
            tp_pct = ((tp - entry) / entry * 100) if entry > 0 else 0
        else:
            sl_pct = ((entry - sl) / entry * 100) if entry > 0 else 0
            tp_pct = ((entry - tp) / entry * 100) if entry > 0 else 0

        lines.append("🎯 <b>STRATEGIYA</b>")
        lines.append(f"  📍 {direction} → Entry: {fmt_price(entry)}$")
        lines.append(f"  🛑 SL: {fmt_price(sl)}$ ({sl_pct:+.1f}%)")
        lines.append(f"  🎯 TP: {fmt_price(tp)}$ ({tp_pct:+.1f}%)")
        if alert.strategy_reason:
            lines.append(f"  💡 {alert.strategy_reason}")
        lines.append("")

    # ── VAQT ─────────────────────────────────────────
    time_str = fmt_time(now, tz_offset)
    lines.append(f"⏱ {time_str} UTC")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# LIVE PNL UPDATE — edit_message_text uchun
# ═══════════════════════════════════════════════════════════════

def build_live_pnl_update(
    signal,
    current_price: float,
    tz_offset: int = 0,
) -> str:
    """Build live PNL update — same format as short signal"""
    now = datetime.utcnow()
    pnl = signal.get_pnl_pct(current_price)
    pnl_ico = "🟢" if pnl > 0 else "🔴" if pnl < 0 else "⚪"

    extra = getattr(signal, "extra_data", {}) or {}

    lines = []
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"🪙 <b>{signal.symbol}</b> — {fmt_price(current_price)}$")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")

    # ── WHALE ────────────────────────────────────────
    whale_usdt = extra.get("whale_liquidity_usdt", 0) or 0
    last_whale_usdt = extra.get("last_whale_usdt", 0) or 0
    last_whale_side = extra.get("last_whale_side", "")
    if whale_usdt > 0:
        buy_v = extra.get("whale_buy_volume", 0) or 0
        sell_v = extra.get("whale_sell_volume", 0) or 0
        bias = "BUY 🟢" if buy_v >= sell_v else "SELL 🔴"
        lines.append(f"🐋 <b>WHALE — {bias}</b>")
        lines.append(f"  {fmt_usdt(whale_usdt)}")
    elif last_whale_usdt > 0:
        dir_word = "BUY 🟢" if last_whale_side == "buy" else "SELL 🔴"
        lines.append(f"🐋 <b>WHALE — {dir_word}</b>")
        lines.append(f"  {fmt_usdt(last_whale_usdt)}")
    lines.append("")

    # ── OI + FUNDING ─────────────────────────────────
    oi_usdt = extra.get("oi_usdt", 0) or 0
    oi_change = extra.get("oi_change_pct", 0) or 0
    funding = extra.get("funding_rate", 0) or 0
    oi_f_parts = []
    if oi_usdt > 0 or oi_change != 0:
        oi_ico = "▲" if oi_change > 0 else "▼"
        oi_f_parts.append(f"📊 OI: {fmt_usdt(oi_usdt)} {oi_ico} {oi_change:+.1f}%")
    if funding != 0:
        fr_ico = "🔴" if funding > 0 else "🟢"
        oi_f_parts.append(f"{fr_ico} Funding: {funding:+.4f}%")
    if oi_f_parts:
        lines.append(" | ".join(oi_f_parts))
        lines.append("")

    # ── ORDERBOOK ────────────────────────────────────
    ob_buy = extra.get("ob_buy_walls", [])
    ob_sell = extra.get("ob_sell_walls", [])
    imbalance = extra.get("ob_imbalance", 1.0)
    buy_price = fmt_price(ob_buy[0]["price"]) if ob_buy else "—"
    buy_usdt_v = fmt_usdt(ob_buy[0]["usdt"]) if ob_buy else "—"
    sell_price = fmt_price(ob_sell[0]["price"]) if ob_sell else "—"
    sell_usdt_v = fmt_usdt(ob_sell[0]["usdt"]) if ob_sell else "—"
    ob_bias = "BUY 🟢" if imbalance >= 1 else "SELL 🔴"
    lines.append("📖 <b>ORDERBOOK</b>")
    lines.append(f"  Buy: {buy_price} ({buy_usdt_v}) | Sell: {sell_price} ({sell_usdt_v})")
    lines.append(f"  Imbalance: {imbalance:.1f}x {ob_bias}")
    lines.append("")

    # ── VOLUME ───────────────────────────────────────
    vol_5m = extra.get("vol_5m", 0) or 0
    vol_1h = extra.get("vol_1h", 0) or 0
    cvd_5m = extra.get("cvd_5m", 0) or 0
    taker = extra.get("taker_ratio", 0) or 0
    c5m = extra.get("price_change_5m", 0) or 0
    c1h = extra.get("price_change_1h", 0) or 0
    c24h = extra.get("price_change_24h", 0) or 0

    vol_parts = []
    if vol_5m > 0:
        vol_parts.append(f"5m: {fmt_usdt(vol_5m)}")
    if vol_1h > 0:
        vol_parts.append(f"1h: {fmt_usdt(vol_1h)}")

    extra_parts = []
    if abs(cvd_5m) > 0:
        cvd_ico = "🟢" if cvd_5m > 0 else "🔴"
        extra_parts.append(f"CVD: {cvd_ico} {fmt_usdt(abs(cvd_5m))}")
    if taker > 0:
        tb = "BUY 🟢" if taker > 1.1 else "SELL 🔴" if taker < 0.9 else "⚪"
        extra_parts.append(f"Taker: {taker:.2f}x {tb}")
    if c5m != 0:
        ico = "🟢" if c5m > 0 else "🔴" if c5m < 0 else "⚪"
        extra_parts.append(f"5m {ico} {c5m:+.2f}%")
    if c1h != 0:
        ico = "🟢" if c1h > 0 else "🔴" if c1h < 0 else "⚪"
        extra_parts.append(f"1h {ico} {c1h:+.2f}%")
    if c24h != 0:
        ico = "🟢" if c24h > 0 else "🔴" if c24h < 0 else "⚪"
        extra_parts.append(f"24h {ico} {c24h:+.2f}%")

    if vol_parts or extra_parts:
        lines.append("💹 <b>VOLUME</b>")
        if vol_parts:
            lines.append(f"  {' | '.join(vol_parts)}")
        if extra_parts:
            lines.append(f"  {' | '.join(extra_parts)}")
        lines.append("")

    # ── PnL + TIME ───────────────────────────────────
    time_str = fmt_time(now, tz_offset)
    lines.append(f"{pnl_ico} PnL: <b>{pnl:+.2f}%</b>")
    lines.append(f"⏱ {signal.get_duration_str()} | {time_str} UTC")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")

    if signal.status in ("tp_hit", "sl_hit"):
        result_ico = "✅ TP HIT" if signal.status == "tp_hit" else "❌ SL HIT"
        lines.append(f"\n{result_ico} — PnL: {signal.pnl_pct:+.2f}%")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# CEXTRACK-STYLE ACTIVITY ALERT
# ═══════════════════════════════════════════════════════════════

def build_cextrack_activity(
    symbol: str,
    event_type: str,
    usdt_value: float,
    duration_min: int,
    pct_of_volume: float = 0,
    direction: str = "",
    extra_info: str = "",
) -> str:
    """
    CEXTrack-style activity alert:
    🎰 #BTC activity 🤔 na 12,61M USDT za 13 min (10%) on Binance Futures
    """
    ico_map = {
        "whale": "🐋",
        "oi": "📈",
        "volume": "💹",
        "liquidation": "💥",
    }
    ico = ico_map.get(event_type, "🔔")
    direction_str = f" {direction.upper()}" if direction else ""

    lines = [
        f"{ico} <b>#{symbol}</b> {event_type}{direction_str}",
        f"💰 {fmt_usdt(usdt_value)} za {duration_min} min",
    ]
    if pct_of_volume > 0:
        lines[1] += f" ({pct_of_volume:.1f}%)"
    lines.append("on Binance Futures")

    if extra_info:
        lines.append(extra_info)

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# MOBILE BOOKMAP — Price-First Vertical
# ═══════════════════════════════════════════════════════════════

def build_bookmap_message(
    symbol: str,
    current_price: float,
    extra: dict,
    price_changes: dict = None,
    liq_clusters: list = None,
) -> str:
    """Mobile-friendly Bookmap — Price-First vertical format"""
    if current_price <= 0:
        return ""

    now = datetime.utcnow().strftime("%H:%M:%S")
    buy_walls = extra.get("ob_buy_walls", [])
    sell_walls = extra.get("ob_sell_walls", [])
    clusters = liq_clusters or []

    # Merge OB + Liq clusters into one sorted list
    price_levels = []

    # Add buy walls
    for w in buy_walls:
        price_levels.append({
            "price": w["price"],
            "type": "buy",
            "usdt": w["usdt"],
            "label": "📖 BUY",
        })

    # Add sell walls
    for w in sell_walls:
        price_levels.append({
            "price": w["price"],
            "type": "sell",
            "usdt": w["usdt"],
            "label": "📖 SELL",
        })

    # Add liq clusters
    for c in clusters:
        label = "🔴 LIQ SHORT" if c["side"] == "short_liq" else "🟢 LIQ LONG"
        price_levels.append({
            "price": c["price"],
            "type": "liq",
            "usdt": c["total_usdt"],
            "label": label,
        })

    if not price_levels:
        return ""

    # Sort by price descending (highest first)
    price_levels.sort(key=lambda x: x["price"], reverse=True)

    lines = [
        "━━━━━━━━━━━━━━━━━━",
        f"📊 <b>BOOKMAP — #{symbol}</b>",
        "━━━━━━━━━━━━━━━━━━",
        "",
    ]

    # Find max usdt for bar scaling
    max_usdt = max(p["usdt"] for p in price_levels) if price_levels else 1

    # Current price index
    current_idx = -1
    for i, p in enumerate(price_levels):
        if p["price"] <= current_price:
            current_idx = i
            break

    # Render: sell walls above, current price in middle, buy walls below
    for i, p in enumerate(price_levels):
        bar_len = int((p["usdt"] / max_usdt) * 10) if max_usdt > 0 else 0
        bar = "█" * bar_len + "░" * (10 - bar_len)
        dist = abs(p["price"] - current_price) / current_price * 100 if current_price > 0 else 0

        # Distance indicator
        if p["price"] > current_price:
            dist_str = f"+{dist:.1f}%"
        elif p["price"] < current_price:
            dist_str = f"-{dist:.1f}%"
        else:
            dist_str = "= 0%"

        line = f"  {fmt_price(p['price'])}$ │{bar}│ {fmt_usdt(p['usdt'])} {dist_str}"

        # Mark current price position
        if i == current_idx:
            lines.append(f"  <b>━━━ NOW: {fmt_price(current_price)}$ ━━━</b>")
            lines.append(line)
        else:
            # Add label
            label = p.get("label", "")
            if label:
                lines.append(f"  {label}")
            lines.append(line)

        # Separator between sells and buys
        if i == current_idx and current_idx > 0:
            pass  # Already marked with NOW line

    lines.append("")
    lines.append(f"⏱ {now} UTC")
    lines.append("━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# KEYBOARDS
# ═══════════════════════════════════════════════════════════════

def build_alert_keyboard(
    symbol: str, exchange: str = "BINANCE", market_type: str = "futures",
    signal_id: str = "",
) -> list:
    tv_symbol = f"{symbol}.P" if market_type == "futures" else symbol
    tv_url = f"https://www.tradingview.com/symbols/{exchange.upper()}-{tv_symbol}/"
    ob_url = f"https://www.binance.com/en/futures/{symbol}"
    return [
        [
            {"text": "📈 TradingView", "url": tv_url},
            {"text": "📚 Order Book", "url": ob_url},
        ],
    ]


def build_signal_keyboard(
    symbol: str, exchange: str = "BINANCE", market_type: str = "futures",
    signal_id: str = "",
) -> list:
    """Keyboard with Refresh + Show More buttons"""
    base = build_alert_keyboard(symbol, exchange, market_type, signal_id)
    # Add Refresh + Show More buttons
    base.append([
        {"text": "🔄 Yangilash", "callback_data": f"refresh:{signal_id}"},
        {"text": "📖 Batafsil", "callback_data": f"showmore:{signal_id}"},
    ])
    return base


def build_showmore_keyboard(signal_id: str) -> list:
    """Keyboard for expanded view — with Qisqartirish button"""
    return [
        [
            {"text": "🔙 Qisqartirish", "callback_data": f"collapse:{signal_id}"},
        ],
    ]


def build_listing_message(symbol, exchange, market_type, is_listing, trading_time="") -> str:
    if is_listing:
        return (
            f"🚀 <b>YANGI LISTING</b>\n\n"
            f"🪙 <b>{symbol}</b> ({exchange} {market_type.upper()})\n"
            f"⏰ Savdo boshlanishi: <b>{trading_time}</b>"
        )
    return (
        f"⚠️ <b>DELISTING</b>\n\n"
        f"🪙 <b>{symbol}</b> ({exchange} {market_type.upper()})\n"
        f"⏰ Savdo to'xtashi: <b>{trading_time}</b>"
    )


def build_top_report(
    top_volume: list,
    top_oi_up: list,
    top_oi_down: list,
    top_liq: list,
    top_m5: list = None,
    top_h1: list = None,
    top_h4: list = None,
) -> str:
    lines = [
        "━━━━━━━━━━━━━━━━━━",
        "📊 <b>TOP COIN REPORT</b>",
        "━━━━━━━━━━━━━━━━━━",
        "",
    ]

    # M5/H1/H4 Top Movers
    if top_m5:
        lines.append("🔥 <b>M5 (5 daqiqa)</b>")
        for i, item in enumerate(top_m5[:5], 1):
            ico = "🟢" if item["change"] > 0 else "🔴"
            lines.append(f"  {i}. {item['symbol']}  {ico} <b>{item['change']:+.2f}%</b>")
        lines.append("")

    if top_h1:
        lines.append("📈 <b>H1 (1 soat)</b>")
        for i, item in enumerate(top_h1[:5], 1):
            ico = "🟢" if item["change"] > 0 else "🔴"
            lines.append(f"  {i}. {item['symbol']}  {ico} <b>{item['change']:+.2f}%</b>")
        lines.append("")

    if top_h4:
        lines.append("🚀 <b>H4 (4 soat)</b>")
        for i, item in enumerate(top_h4[:5], 1):
            ico = "🟢" if item["change"] > 0 else "🔴"
            lines.append(f"  {i}. {item['symbol']}  {ico} <b>{item['change']:+.2f}%</b>")
        lines.append("")

    if top_oi_up:
        lines.append("<b>📈 OI O'SISH (TOP 5)</b>")
        for i, item in enumerate(top_oi_up[:5], 1):
            lines.append(f"  {i}. {item['symbol']}  <b>{item['change']:+.1f}%</b>")
        lines.append("")

    if top_oi_down:
        lines.append("<b>📉 OI TUSHISH (TOP 5)</b>")
        for i, item in enumerate(top_oi_down[:5], 1):
            lines.append(f"  {i}. {item['symbol']}  <b>{item['change']:+.1f}%</b>")
        lines.append("")

    if top_liq:
        lines.append("<b>💥 LIKVIDATSIYALAR (TOP 5)</b>")
        for i, item in enumerate(top_liq[:5], 1):
            lines.append(f"  {i}. {item['symbol']}  <b>{fmt_usdt(item['total'])}</b>")
        lines.append("")

    if top_volume:
        lines.append("<b>💹 FAOL HAJM (TOP 5)</b>")
        for i, item in enumerate(top_volume[:5], 1):
            lines.append(f"  {i}. {item['symbol']}  <b>{fmt_usdt(item['usdt'])}</b>")
        lines.append("")

    lines.append("━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)


# Eski nomlar bilan moslik
build_alert_message = build_full_signal
build_alert_message_v2 = build_full_signal
