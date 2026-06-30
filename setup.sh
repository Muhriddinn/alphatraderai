#!/bin/bash
# ═══════════════════════════════════════════════════════════
#   ALPHATRADERAI — AVTOMATIK O'RNATISH SKRIPTI
#   Buni serverda 1 marta ishga tushiring, hammasi tayyor bo'ladi
# ═══════════════════════════════════════════════════════════

echo "═══════════════════════════════════════════════"
echo "   ALPHATRADERAI — O'RNATISH BOSHLANDI"
echo "═══════════════════════════════════════════════"

# 1. Serverni yangilash
echo ""
echo "[1/7] Server yangilanmoqda..."
sudo apt update -y && sudo apt upgrade -y

# 2. Python va kerakli narsalar
echo ""
echo "[2/7] Python o'rnatilmoqda..."
sudo apt install python3 python3-pip python3-venv screen -y

# 3. Bot papkasini yaratish
echo ""
echo "[3/7] Papka yaratilmoqda..."
mkdir -p ~/ALPHATRADERAI
cd ~/ALPHATRADERAI

# 4. Virtual muhit yaratish
echo ""
echo "[4/7] Virtual muhit yaratilmoqda..."
python3 -m venv venv
source venv/bin/activate

# 5. Bot fayllarini kutish
echo ""
echo "[5/7] Bot fayllari kutilmoqda..."
echo ""
echo "═══════════════════════════════════════════════"
echo "   ENDI BOT FAYLLARINI YUKLANG!"
echo "═══════════════════════════════════════════════"
echo ""
echo "WinSCP bilan C:\ALPHATRADERAI papkasidagi"
echo "HAMMA FAYLLARNI shu joyga yuklang:"
echo ""
echo "   ~/ALPHATRADERAI/"
echo ""
echo "Yuklab bo'lgach, ENTER bosing..."
read -p ""

# 6. Kutubxonalarni o'rnatish
echo ""
echo "[6/7] Kutubxonalar o'rnatilmoqda..."
pip install -r requirements.txt

# 7. Avtomatik ishga tushirish (systemd)
echo ""
echo "[7/7] Avtomatik ishga tushirilmoqda..."

sudo tee /etc/systemd/system/alphatrader.service > /dev/null << 'EOF'
[Unit]
Description=ALPHATRADERAI Bot
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$HOME/ALPHATRADERAI
ExecStart=$HOME/ALPHATRADERAI/venv/bin/python main.py
Restart=always
RestartSec=10
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONIOENCODING=utf-8

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable alphatrader
sudo systemctl start alphatrader

echo ""
echo "═══════════════════════════════════════════════"
echo "   O'RNATISH TUGADI!"
echo "═══════════════════════════════════════════════"
echo ""
echo "Bot holatini tekshirish:"
echo "   sudo systemctl status alphatrader"
echo ""
echo "Botni to'xtatish:"
echo "   sudo systemctl stop alphatrader"
echo ""
echo "Botni qayta ishga tushirish:"
echo "   sudo systemctl restart alphatrader"
echo ""
echo "Loglarni ko'rish:"
echo "   sudo journalctl -u alphatrader -f"
echo ""
echo "Telegram dan sinash:"
echo "   /start"
echo "   /liqmap"
echo "   /predict BTCUSDT"
echo ""
echo "═══════════════════════════════════════════════"
echo "   BOT 1 OY AVTOMATIK ISHLAYDI!"
echo "═══════════════════════════════════════════════"
