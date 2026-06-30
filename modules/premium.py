"""
ALPHATRADERAI — Premium Access Checker
Foydalanuvchining premium holatini tekshiradi.
Trial, premium, oddiy — barcha holatlarni boshqaradi.
"""
from datetime import datetime, timedelta
from loguru import logger

ADMIN_IDS = [5571433323, 1101182189]

TRIAL_DAYS = 3
SIGNAL_LIMIT_FREE = 1
SIGNAL_LIMIT_PREMIUM = 999

WALLET_ADDRESS = "HOZIRCHA YO'Q"

PLANS = {
    "1m": {"name": "1 oy", "days": 30, "price_usdt": 30},
    "3m": {"name": "3 oy", "days": 90, "price_usdt": 75},
    "12m": {"name": "12 oy", "days": 365, "price_usdt": 250},
}

# ════════════════════════════════════════════
# GLOBAL FREE TRIAL — IYUL OYI UCHUN HAMMA UCHUN BEPUL
# ════════════════════════════════════════════
GLOBAL_FREE_START = datetime(2026, 7, 1)
GLOBAL_FREE_END = datetime(2026, 7, 31, 23, 59, 59)
GLOBAL_FREE_ENABLED = True


def is_global_free_period() -> bool:
    """Hozir global bepul davrmi? (Iyul 2026)"""
    if not GLOBAL_FREE_ENABLED:
        return False
    now = datetime.utcnow()
    return GLOBAL_FREE_START <= now <= GLOBAL_FREE_END


def get_global_free_remaining() -> dict:
    """Global bepul davrdan qolgan vaqt"""
    now = datetime.utcnow()
    if not is_global_free_period():
        return {"active": False, "days": 0, "hours": 0}
    
    remaining = GLOBAL_FREE_END - now
    return {
        "active": True,
        "days": remaining.days,
        "hours": remaining.seconds // 3600,
    }


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def check_premium(session, user_id: int) -> dict:
    from db.models import User
    from sqlalchemy import select

    if is_admin(user_id):
        return {
            "is_premium": True,
            "is_admin": True,
            "status": "admin",
            "expires_at": None,
            "signal_limit": SIGNAL_LIMIT_PREMIUM,
            "features": "all",
        }

    # Global bepul davr — hamma uchun premium
    if is_global_free_period():
        remaining = get_global_free_remaining()
        return {
            "is_premium": True,
            "is_admin": False,
            "status": "global_free",
            "expires_at": GLOBAL_FREE_END,
            "remaining_days": remaining["days"],
            "remaining_hours": remaining["hours"],
            "signal_limit": SIGNAL_LIMIT_PREMIUM,
            "features": "all",
        }

    result = await session.execute(
        select(User).where(User.telegram_id == user_id)
    )
    user = result.scalar_one_or_none()

    if not user:
        return {
            "is_premium": False,
            "is_admin": False,
            "status": "unknown",
            "expires_at": None,
            "signal_limit": 0,
            "features": "none",
        }

    now = datetime.utcnow()

    if user.is_premium and user.premium_expires_at and user.premium_expires_at > now:
        return {
            "is_premium": True,
            "is_admin": False,
            "status": "premium",
            "expires_at": user.premium_expires_at,
            "signal_limit": SIGNAL_LIMIT_PREMIUM,
            "features": "all",
        }

    if user.trial_expires_at and user.trial_expires_at > now:
        remaining = user.trial_expires_at - now
        return {
            "is_premium": True,
            "is_admin": False,
            "status": "trial",
            "expires_at": user.trial_expires_at,
            "remaining_days": remaining.days,
            "remaining_hours": remaining.seconds // 3600,
            "signal_limit": SIGNAL_LIMIT_PREMIUM,
            "features": "all",
        }

    if user.is_premium and user.premium_expires_at and user.premium_expires_at <= now:
        user.is_premium = False
        await session.commit()

    return {
        "is_premium": False,
        "is_admin": False,
        "status": "expired",
        "expires_at": user.premium_expires_at,
        "signal_limit": SIGNAL_LIMIT_FREE,
        "features": "basic",
    }


async def check_signal_limit(session, user_id: int) -> bool:
    from db.models import User
    from sqlalchemy import select

    if is_admin(user_id):
        return True

    result = await session.execute(
        select(User).where(User.telegram_id == user_id)
    )
    user = result.scalar_one_or_none()
    if not user:
        return False

    now = datetime.utcnow()

    if user.signal_count_hour_reset is None or (now - user.signal_count_hour_reset).total_seconds() > 3600:
        user.signal_count_hour = 0
        user.signal_count_hour_reset = now
        await session.commit()

    access = await check_premium(session, user_id)
    limit = access["signal_limit"]

    if user.signal_count_hour >= limit:
        return False

    user.signal_count_hour += 1
    await session.commit()
    return True


async def activate_premium(session, user_id: int, days: int):
    from db.models import User
    from sqlalchemy import select

    result = await session.execute(
        select(User).where(User.telegram_id == user_id)
    )
    user = result.scalar_one_or_none()
    if not user:
        return False

    now = datetime.utcnow()
    if user.is_premium and user.premium_expires_at and user.premium_expires_at > now:
        user.premium_expires_at = user.premium_expires_at + timedelta(days=days)
    else:
        user.premium_expires_at = now + timedelta(days=days)

    user.is_premium = True
    user.trial_used = True
    await session.commit()

    logger.info(f"Premium yoqildi: {user_id} — {days} kun")
    return True


async def start_trial(session, user_id: int):
    from db.models import User
    from sqlalchemy import select

    result = await session.execute(
        select(User).where(User.telegram_id == user_id)
    )
    user = result.scalar_one_or_none()
    if not user:
        return False

    if user.trial_used:
        return False

    now = datetime.utcnow()
    user.trial_used = True
    user.trial_expires_at = now + timedelta(days=TRIAL_DAYS)
    user.is_premium = True
    await session.commit()

    logger.info(f"Sinov boshlandi: {user_id} — {TRIAL_DAYS} kun")
    return True


async def get_premium_status_text(session, user_id: int) -> str:
    access = await check_premium(session, user_id)

    if access["status"] == "admin":
        return "👑 <b>ADMIN</b> — Barcha imtiyozlar ochiq"

    if access["status"] == "global_free":
        remaining = get_global_free_remaining()
        return (
            f"🎉 <b>IYUL OYI — HAMMA UCHUN BEPUL!</b>\n"
            f"📅 Qolgan: {remaining['days']} kun {remaining['hours']} soat\n"
            f"📊 Barcha premium imtiyozlar ochiq\n\n"
            f"💡 Iyul oxirida pullik rejaga o'tadi"
        )

    if access["status"] == "premium":
        expires = access["expires_at"]
        remaining = expires - datetime.utcnow()
        days = remaining.days
        hours = remaining.seconds // 3600
        return (
            f"⭐ <b>PREMIUM FAOL</b>\n"
            f"📅 Qolgan: {days} kun {hours} soat\n"
            f"📊 Cheksiz signal"
        )

    if access["status"] == "trial":
        expires = access["expires_at"]
        remaining = expires - datetime.utcnow()
        days = remaining.days
        hours = remaining.seconds // 3600
        return (
            f"🎁 <b>3 KUNLIK SINOV FAOL</b>\n"
            f"📅 Qolgan: {days} kun {hours} soat\n"
            f"📊 Barcha premium imtiyozlar ochiq\n\n"
            f"💡 Sinov tugagach, /premium orqali sotib oling!"
        )

    if access["status"] == "expired":
        return (
            f"⚠️ <b>Premium tugadi</b>\n\n"
            f"Hozir oddiy rejimdasiz:\n"
            f"• {SIGNAL_LIMIT_FREE} ta signal/soat\n"
            f"• Faqat asosiy signallar\n\n"
            f"⭐ Premium sotib oling:\n"
            f"• Cheksiz signal\n"
            f"• Barcha imtiyozlar\n\n"
            f"/premium — narxlar"
        )

    return (
        f"⚠️ <b>Holat noma'lum</b>\n"
        f"/start ni bosib qayta urinib ko'ring"
    )
