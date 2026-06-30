# 🔧 XATOLIKLARNI BARTARAF ETISH

## ❌ XATO: "ModuleNotFoundError"

**Yechim:**
```bash
pip install -r requirements.txt
```

## ❌ XATO: "Permission denied"

**Yechim:**
```bash
chmod +x main.py
```

## ❌ XATO: "Port already in use"

**Yechim:**
```bash
pkill -f "python main.py"
python main.py
```

## ❌ XATO: "Database locked"

**Yechim:**
```bash
rm crypto_monitor.db
python main.py
```

## ❌ XATO: "Token invalid"

**Yechim:**
1. @BotFather ga boring
2. /mybots → Token ni o'zgartiring
3. `.env` faylida yangilang

## ❌ BOT ISHLAMAYAPTI

1. Log tekshiring: `cat logs/bot_err.log`
2. Python versiyasini tekshiring: `python --version`
3. Kutubxonalarni tekshiring: `pip list`

## ❌ SERVERGA ULANMAYAPTI

1. Internet aloqasini tekshiring
2. Server statusini tekshiring
3. DNS ni tekshiring

---

## 📞 YORDAM

Muammo bo'lsa, log faylini yuboring:
```bash
cat logs/bot_err.log > error.txt
```
