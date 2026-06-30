# 🚀 RENDER.COM — TO'LIQ DEPLOYMENT QOLLANMA

## 📋 SERVER: render.com (750 soat/bepul)

---

## 1-QADAM: RENDER.COM GA RO'YXATDAN O'TISH

1. https://render.com ga boring
2. **Get Started for Free** bosing
3. **Sign up with GitHub** bosing (eng oson)
4. GitHub account bilan kiring
5. Email tasdiqlang

---

## 2-QADAM: GITHUB GA REPO YARATISH

### 2.1. GitHub da yangi repo yaratish
1. https://github.com ga boring
2. **New repository** bosing
3. Repository name: `alphatraderai`
4. **Public** tanlang
5. **Create repository** bosing

### 2.2. Git o'rnatish (agar yo'q bo'lsa)
```bash
# Windows uchun
winget install Git.Git

# Yoki https://git-scm.com/downloads dan yuklab oling
```

### 2.3. Papkani GitHub ga yuklash
**PowerShell** da buyruqlarni kiriting:

```bash
# Papkaga boring
cd C:\ALPHATRADERAI

# Git ni ishga tushiring
git init
git add .
git commit -m "First commit"

# GitHub ga ulang
git remote add origin https://github.com/YOUR_USERNAME/alphatraderai.git
git branch -M main
git push -u origin main
```

> ⚠️ `YOUR_USERNAME` o'rniga haqiqiy GitHub username ni kiriting

---

## 3-QADAM: RENDER.COM DA DEPLOY QILISH

### 3.1. New Web Service
1. https://dashboard.render.com ga kiring
2. **New** → **Web Service** bosing
3. **Build and deploy from a Git repository** tanlang
4. **Next** bosing

### 3.2. Repo ni tanlash
1. **Connect GitHub** bosing
2. `alphatraderai` repo ni tanlang
3. **Connect** bosing

### 3.3. Sozlamalar
Quyidagini kiriting:

| Field | Qiymat |
|-------|--------|
| **Name** | `alphatraderai` |
| **Region** | `Oregon (US West)` |
| **Branch** | `main` |
| **Runtime** | `Python 3` |
| **Build Command** | `pip install -r requirements.txt` |
| **Start Command** | `python main.py` |
| **Instance Type** | `Free` |

### 3.4. Environment Variables
**Advanced** → **Add Environment Variable** bosing va quyidagini kiriting:

```
TELEGRAM_BOT_TOKEN = 8574179914:AAHExl7HJm_KwGbNMMa143mtoYAgUo_cUr4
TELEGRAM_ADMIN_IDS = 5571433323,1101182189
DATABASE_URL = sqlite:///crypto_monitor.db
LOG_LEVEL = INFO
ENVIRONMENT = production
MAX_SYMBOLS = 1500
ADMIN_PORT = 8080
```

### 3.5. Deploy
1. **Create Web Service** bosing
2. Render avtomatik deploy qiladi
3. 5-10 daqiqa kutin

---

## 4-QADAM: BOTNI TEKSHIRISH

### 4.1. Loglarni ko'rish
1. Render dashboard → **Logs** bosing
2. Xatoliklar borligini tekshiring

### 4.2. Botni tekshirish
1. Telegram da botni toping
2. `/start` bosing
3. `/stats` bosing

### 4.3. URL ni olish
1. Render dashboard → **Settings** bosing
2. **URL** ni ko'ring (masalan: `https://alphatraderai.onrender.com`)

---

## 5-QADAM: AUTO-DEPLOY SOZLASH

### 5.1. Auto Deploy yoqish
1. Render dashboard → **Settings** bosing
2. **Auto Deploy** → **Yes** tanlang
3. **Save** bosing

Endi GitHub ga yangi kod yuklasangiz, avtomatik deploy bo'ladi.

---

## ⚠️ MUHIM ESLATMALAR

### 1. Free Plan cheklovlari
- **750 soat/oy** (25 kun)
- **15 daqiqa** idle bo'lsa → to'xtaydi
- **512 MB RAM**
- **0.5 CPU**

### 2. Bot 24/7 ishlashi uchun
Free plan da bot 15 daqiqadan keyin to'xtaydi. Buni hal qilish uchun:

**Variant 1: UptimeRobot (BEPUL)**
1. https://uptimerobot.com ga boring
2. Ro'yxatdan o'ting
3. **Add New Monitor** bosing
4. **Monitor Type:** HTTP(s)
5. **URL:** `https://alphatraderai.onrender.com`
6. **Monitoring Interval:** 5 minutes
7. **Create Monitor** bosing

Bu bot har 5 daqiqada "wake" qiladi.

**Variant 2: Render Pro ($7/oy)**
- 24/7 uptime
- Tezroq ishlaydi
- Ko'proq resurslar

### 3. Database
SQLite ishlatish tavsiya etiladi (hozirgi holat).

### 4. Xatoliklar
Agar bot ishlamasa:
1. **Logs** ni tekshiring
2. **Environment Variables** to'g'ri ekanligini tekshiring
3. **requirements.txt** mavjudligini tekshiring

---

## 📋 DEPLOYMENT CHECKLIST

- [ ] Render.com ro'yxatdan o'tildi
- [ ] GitHub repo yaratildi
- [ ] Kod GitHub ga yuklandi
- [ ] Render.com da Web Service yaratildi
- [ ] Environment Variables qo'shildi
- [ ] Deploy muvaffaqiyatli bo'ldi
- [ ] Bot ishga tushdi
- [ ] UptimeRobot sozlandi (24/7 uchun)

---

## 🔧 YORDAM

Agar muammo bo'lsa:
1. Render **Logs** ni tekshiring
2. GitHub **commits** ni tekshiring
3. **Environment Variables** ni tekshiring
4. **requirements.txt** ni tekshiring
