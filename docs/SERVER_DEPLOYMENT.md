# 🚀 ALPHATRADERAI — SERVERGA DEPLOYMENT QOLLANMA

## 📋 SERVER: smarterasp.net (60 kun bepul)

---

## 1-QADAM: SERVER RO'YXATDAN O'TISH

1. https://www.smarterasp.net ga boring
2. **Sign Up** tugmasini bosing
3. Email va parol kiriting
4. Email tasdiqlang
5. **Free Plan** ni tanlang (60 kun bepul)

---

## 2-QADAM: HOSTING PANELINI SOZLASH

1. **Control Panel** ga kiring
2. **Websites** → **Create Website** bosing
3. Domain nomini kiriting (masalan: `alphatraderai.com`)
4. **Create** bosing

---

## 3-QADAM: PYTHON QAYTA ISHLATISH

### 3.1. Python versiyasini tanlash
1. **Settings** → **Python Version** bosing
2. **Python 3.11** yoki **3.12** tanlang
3. **Save** bosing

### 3.2. Virtual Environment yaratish
1. **SSH Terminal** ni oching yoki **File Manager** dan foydalaning
2. Quyidagi buyruqlarni kiriting:

```bash
# Python virtual environment yaratish
python -m venv venv

# Virtual environment ni yoqish
source venv/bin/activate
```

---

## 4-QADAM: FAYLLARNI YUKLASH

### 4.1. FTP/SFTP orqali yuklash
1. **File Manager** ni oching
2. Quyidagi papkalarni yuklang:

```
/
├── main.py              ← Asosiy bot
├── requirements.txt     ← Kutubxonalar ro'yxati
├── .env                 ← Maxfiy ma'lumotlar
├── start_bot.bat        ← Windows uchun (kerak emas)
├── bot/                 ← Bot modullari
├── core/                ← Asosiy modullar
├── modules/             ← Qo'shimcha modullar
├── db/                  ← Ma'lumotlar bazasi
├── config/              ← Sozlamalar
└── data/                ← Ma'lumotlar
```

### 4.2. .env faylini yaratish
**File Manager** dan `.env` faylini yarating yoki tahrirlang:

```env
# Telegram
TELEGRAM_BOT_TOKEN=8574179914:AAHExl7HJm_KwGbNMMa143mtoYAgUo_cUr4
TELEGRAM_ADMIN_IDS=5571433323,1101182189

# Binance (ixtiyoriy)
BINANCE_API_KEY=
BINANCE_API_SECRET=

# Bybit (ixtiyoriy)
BYBIT_API_KEY=
BYBIT_API_SECRET=

# OKX (ixtiyoriy)
OKX_API_KEY=
OKX_API_SECRET=
OKX_PASSPHRASE=

# Database
DATABASE_URL=sqlite:///crypto_monitor.db

# System
LOG_LEVEL=INFO
ENVIRONMENT=production
MAX_SYMBOLS=1500
ADMIN_PORT=8080
```

---

## 5-QADAM: KUTUBXONALARNI O'R NATISH

**SSH Terminal** da buyruqlarni kiriting:

```bash
# Virtual environment ni yoqish
source venv/bin/activate

# Kutubxonalarni o'rnatish
pip install -r requirements.txt

# Agar xatolik chiqsa, alohida o'rnating:
pip install python-telegram-bot
pip install aiohttp
pip install websockets
pip install pydantic
pip install pydantic-settings
pip install python-dotenv
pip install loguru
pip install aiosqlite
pip install sqlalchemy
pip install numpy
```

---

## 6-QADAM: BOTNI ISHGA TUSHIRISH

### 6.1. Start Command sozlash
1. **Settings** → **Startup Command** bosing
2. Quyidagini kiriting:

```bash
cd /home/username && source venv/bin/activate && python main.py
```

> ⚠️ `username` o'rniga haqiqiy username ni kiriting

### 6.2. Botni qayta ishga tushirish
**SSH Terminal** da:

```bash
# Botni to'xtatish
pkill -f "python main.py"

# Botni ishga tushirish
cd /home/username
source venv/bin/activate
python main.py
```

---

## 7-QADAM: BOTNI QAYTA ISHGA TUSHIRISH (AUTO-START)

### 7.1. Procfile yaratish
**File Manager** dan `Procfile` nomli fayl yarating:

```
web: python main.py
```

### 7.2. Yoki systemd service (Linux)
Agar Linux server bo'lsa:

```bash
# /etc/systemd/system/alphatraderai.service fayl yarating
sudo nano /etc/systemd/system/alphatraderai.service
```

Quyidagini kiriting:

```ini
[Unit]
Description=AlphaTraderAI Bot
After=network.target

[Service]
User=username
WorkingDirectory=/home/username
ExecStart=/home/username/venv/bin/python main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Saqlang va ishga tushiring:

```bash
sudo systemctl daemon-reload
sudo systemctl enable alphatraderai
sudo systemctl start alphatraderai
```

---

## 8-QADAM: XATOLIKLARNI TEKSHIRISH

### 8.1. Log fayllarini ko'rish
```bash
# Xatolik loglari
cat logs/bot_err.log

# Asosiy loglar
cat logs/bot_out.log

# Yoki real-vaqtda
tail -f logs/bot_out.log
```

### 8.2. Bot ishlayaptimi tekshirish
```bash
# Bot jarayonini tekshirish
ps aux | grep python

# Portni tekshirish
netstat -tlnp | grep 8080
```

---

## 9-QADAM: DATABASE SOZLASH

### 9.1. SQLite (hozirgi holat)
Bot avtomatik ravishda `crypto_monitor.db` faylini yaratadi.

### 9.2. PostgreSQL (ixtiyoriy, yaxshiroq)
Agar PostgreSQL kerak bo'lsa:

1. **Database** → **PostgreSQL** bosing
2. **Create Database** bosing
3. `.env` faylida o'zgartiring:

```env
DATABASE_URL=postgresql://username:password@localhost:5432/alphatraderai
```

---

## 10-QADAM: TESTING

### 10.1. Botni tekshirish
1. Telegram da botni toping
2. `/start` bosing
3. `/stats` bosing
4. `/admin` bosing (adminlar uchun)

### 10.2. Xatoliklar
Agar bot ishlamasa:

1. Log fayllarini tekshiring
2. `.env` faylini tekshiring
3. Kutubxonalar to'g'ri o'rnatilganini tekshiring
4. Python versiyasini tekshiring

---

## 📋 DEPLOYMENT CHECKLIST

- [ ] Server ro'yxatdan o'tildi (smarterasp.net)
- [ ] Hosting panel sozlandi
- [ ] Python 3.11/3.12 tanlandi
- [ ] Virtual environment yaratildi
- [ ] Fayllar yuklandi
- [ ] `.env` fayli yaratildi
- [ ] Kutubxonalar o'rnatildi
- [ ] Bot ishga tushirildi
- [ ] Bot tekshirildi

---

## ⚠️ MUHIM ESLATMALAR

1. **Token xavfsizligi**: `.env` faylini hech qachon GitHub ga yuklamang
2. **Port**: SmarterASP.net da port o'zgarmaydi (80 yoki 443)
3. **Database**: SQLite ishlatish tavsiya etiladi (oddiy)
4. **Log**: Xatolik loglarini muntazam tekshiring
5. **Backup**: Ma'lumotlar bazasini muntazam zaxiralang

---

## 🔧 YORDAM

Agar muammo bo'lsa:
1. Log fayllarini tekshiring
2. `requirements.txt` ni tekshiring
3. `.env` faylini tekshiring
4. Python versiyasini tekshiring
