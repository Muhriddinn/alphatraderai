"""
CRYPTO MONITOR PRO — Configuration (EARLY WARNING — MINIMAL THRESHOLDS)
Maqsad: Bozorda harakat BOSHLANAYOTGANIDA darhol signal berish
"""
from pydantic_settings import BaseSettings
from pydantic import Field
from dotenv import load_dotenv
load_dotenv()


class Settings(BaseSettings):
    # Telegram
    telegram_bot_token: str = Field(..., env="TELEGRAM_BOT_TOKEN")
    telegram_admin_ids: str = Field(default="", env="TELEGRAM_ADMIN_IDS")

    # Binance
    binance_api_key: str = Field(default="", env="BINANCE_API_KEY")
    binance_api_secret: str = Field(default="", env="BINANCE_API_SECRET")

    # Bybit
    bybit_api_key: str = Field(default="", env="BYBIT_API_KEY")
    bybit_api_secret: str = Field(default="", env="BYBIT_API_SECRET")

    # OKX
    okx_api_key: str = Field(default="", env="OKX_API_KEY")
    okx_api_secret: str = Field(default="", env="OKX_API_SECRET")
    okx_passphrase: str = Field(default="", env="OKX_PASSPHRASE")

    # Database
    database_url: str = Field(
        default="postgresql+asyncpg://postgres:password@localhost:5432/crypto_monitor",
        env="DATABASE_URL"
    )
    redis_url: str = Field(default="redis://localhost:6379/0", env="REDIS_URL")

    # Web Admin
    admin_secret_key: str = Field(default="supersecret", env="ADMIN_SECRET_KEY")
    admin_port: int = Field(default=8080, env="ADMIN_PORT")

    # System
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    environment: str = Field(default="development", env="ENVIRONMENT")
    max_symbols: int = Field(default=1500, env="MAX_SYMBOLS")
    ws_reconnect_delay: int = Field(default=3, env="WS_RECONNECT_DELAY")
    ws_max_retries: int = Field(default=10, env="WS_MAX_RETRIES")

    # ══════════════════════════════════════════════
    # EARLY WARNING THRESHOLDS
    # Maqsad: Harakat BOSHLANAYOTGANDA signal berish
    # ══════════════════════════════════════════════

    # Volume — 30% oshsa yetarli (kanal 10-50% ko'rsatadi)
    volume_spike_threshold: float = 30.0
    volume_whale_min_usdt: float = 50_000      # 50K$ = sezilarli

    # Open Interest — 0.5% oshsa yangi pozitsiyalar ochilmoqda
    oi_spike_threshold: float = 0.5
    oi_rapid_seconds: int = 10
    oi_rapid_threshold: float = 0.2

    # Liquidation — 5K$ dan boshlab (kichik ham muhim)
    liq_min_usdt: float = 5_000
    liq_wave_window: int = 60

    # Whale — 50K$ dan boshlab
    whale_order_min_usdt: float = 50_000
    whale_burst_min_usdt: float = 100_000

    # Funding — standart 0.01%, biz 0.015% da signal
    funding_extreme_positive: float = 0.015
    funding_extreme_negative: float = -0.015

    # Order Book — 100K$ devor
    orderbook_wall_min_usdt: float = 100_000
    orderbook_imbalance_ratio: float = 1.5

    # Ball tizimi — faqat ko'rsatish uchun, filter emas
    score_notice: int = 1      # hamma signal o'tsin
    score_strong: int = 20
    score_extreme: int = 40

    class Config:
        env_file = ".env"
        extra = "ignore"

    @property
    def admin_ids_list(self) -> list[int]:
        if isinstance(self.telegram_admin_ids, str):
            return [int(x.strip()) for x in self.telegram_admin_ids.split(",") if x.strip()]
        return self.telegram_admin_ids


settings = Settings()