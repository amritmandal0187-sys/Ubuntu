# ============================================
# GOOGLE CLOUD SHELL - COMPLETE AUTO SETUP
# ============================================

cat > ~/setup_cloudshell.sh << 'CLOUDSETUP'
#!/bin/bash

echo "========================================="
echo "  RK AUTOMATION - CLOUD SHELL SETUP"
echo "========================================="

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Step 1: Install Python packages
echo -e "${YELLOW}[1/5] Installing Python packages...${NC}"
pip3 install --user playwright requests pyotp --break-system-packages 2>/dev/null || pip3 install --user playwright requests pyotp

# Step 2: Install Playwright browsers
echo -e "${YELLOW}[2/5] Installing Playwright Chromium...${NC}"
export PLAYWRIGHT_BROWSERS_PATH=$HOME/.cache/ms-playwright
python3 -m playwright install chromium

# Step 3: Create directory
echo -e "${YELLOW}[3/5] Creating workspace...${NC}"
mkdir -p ~/rk-automation
mkdir -p ~/rk-automation/screenshots

# Step 4: Create automation script
echo -e "${YELLOW}[4/5] Creating automation script...${NC}"

cat > ~/rk-automation/cloud_automation.py << 'AUTOSCRIPT'
#!/usr/bin/env python3
"""
RK DIGITAL AUTOMATION - GOOGLE CLOUD SHELL OPTIMIZED
"""

import os
import time
import socket
import requests
import pyotp
import sys
import threading
from datetime import datetime
from playwright.sync_api import sync_playwright

# ==================== CONFIGURATION ====================
API_URL = "http://13.235.87.209:3102"
USER_NAME = os.getenv("USER", "cloudshell")
WORKER_ID = f"CLOUD_{USER_NAME}_{socket.gethostname()}"
HEARTBEAT_INTERVAL = 10
POLL_INTERVAL = 15

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "worker.log")
SCREENSHOT_DIR = os.path.join(BASE_DIR, "screenshots")
os.makedirs(SCREENSHOT_DIR, exist_ok=True)

# ==================== STATE ====================
state = {
    "leads_done": 0,
    "leads_failed": 0,
    "current_action": "Starting up...",
    "paused": False,
    "stop_requested": False,
}

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "Unknown"

LOCAL_IP = get_local_ip()

def log(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] [{WORKER_ID}] [{level}] {message}"
    print(log_line)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(log_line + "\n")
    except:
        pass

def heartbeat_loop():
    while not state["stop_requested"]:
        try:
            requests.post(f"{API_URL}/api/heartbeat", json={
                "worker_id": WORKER_ID,
                "ip_address": LOCAL_IP,
                "leads_done": state["leads_done"],
                "leads_failed": state["leads_failed"],
                "current_action": state["current_action"]
            }, timeout=5)
        except:
            pass
        time.sleep(HEARTBEAT_INTERVAL)

def fetch_task():
    try:
        response = requests.get(
            f"{API_URL}/api/task?worker_id={WORKER_ID}&ip_address={LOCAL_IP}",
            timeout=15
        )
        return response.json()
    except:
        return None

def report_status(email, status, campaign_id=None):
    try:
        payload = {"email": email, "status": status, "worker_id": WORKER_ID}
        if campaign_id:
            payload["campaign_id"] = campaign_id
        requests.post(f"{API_URL}/report", json=payload, timeout=5)
    except:
        pass

def take_screenshot(page, name):
    try:
        filename = os.path.join(SCREENSHOT_DIR, f"{name}_{int(time.time())}.png")
        page.screenshot(path=filename)
        log(f"Screenshot: {filename}")
    except:
        pass

def start_automation(account, leads, settings, campaign=None):
    # Cloud Shell mein headless mode ON
    headless = True
    
    log(f"Starting: {account['email']} ({len(leads)} leads)")
    state["current_action"] = f"Logging in as {account['email']}"
    
    browser_args = [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu"
    ]
    
    with sync_playwright() as p:
        browser = None
        try:
            browser = p.chromium.launch(headless=headless, args=browser_args)
            context = browser.new_context(
                viewport={'width': 1280, 'height': 720},
                user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
            )
            page = context.new_page()
            
            # Login
            log("Logging into Microsoft...")
            page.goto("https://login.microsoftonline.com/", timeout=60000)
            page.fill('input[type="email"]', account['email'])
            page.click('input[type="submit"]')
            time.sleep(2)
            
            page.wait_for_selector('input[type="password"]', timeout=30000)
            page.fill('input[type="password"]', account['password'])
            page.click('input[type="submit"]')
            time.sleep(3)
            
            # Stay signed in
            try:
                if page.is_visible('input[id="idSIButton9"]'):
                    
                    time.sleep(1)
            except:
                pass
            
            # Security info
            log("Opening Security Info...")
            page.goto("https://mysignins.microsoft.com/security-info", timeout=60000)
            time.sleep(5)
            take_screenshot(page, "security_info")
            
            # TOTP if needed
            try:
                if page.is_visible('input[type="tel"]'):
                    log("Entering TOTP...")
                    totp = pyotp.TOTP(account['totp_secret'])
                    page.fill('input[type="tel"]', totp.now())
                    page.click('input[type="submit"]')
                    time.sleep(5)
            except:
                pass
            
            # Process leads
            for idx, email in enumerate(leads):
                if state["stop_requested"]:
                    break
                while state["paused"]:
                    time.sleep(2)
                
                log(f"Processing: {email} ({idx+1}/{len(leads)})")
                
                try:
                    # Click Add button
                    add_btns = ['button:has-text("Add")', 'i[data-icon-name="Add"]']
                    for btn in add_btns:
                        try:
                            if page.is_visible(btn):
                                page.click(btn)
                                break
                        except:
                            continue
                    
                    time.sleep(2)
                    
                    # Select email
                    email_opts = ['[data-testid="authmethod-picker-email"]', 'text=Email']
                    for opt in email_opts:
                        try:
                            if page.is_visible(opt):
                                page.click(opt)
                                break
                        except:
                            continue
                    
                    time.sleep(2)
                    
                    # Fill email
                    page.fill('input[type="email"]', email)
                    page.click('button:has-text("Next")')
                    
                    # Wait for OTP (success)
                    time.sleep(5)
                    otp_found = page.locator('input[type="tel"]').count() > 0
                    
                    if otp_found:
                        log(f"SUCCESS: {email}")
                        report_status(email, "success", campaign.get("id") if campaign else None)
                        state["leads_done"] += 1
                        
                        # Back button
                        try:
                            page.click('button:has-text("Back")')
                        except:
                            pass
                    else:
                        raise Exception("OTP not found")
                    
                    time.sleep(2)
                    
                except Exception as e:
                    log(f"FAILED: {email} - {str(e)[:50]}")
                    report_status(email, "failed", campaign.get("id") if campaign else None)
                    state["leads_failed"] += 1
                    page.goto("https://mysignins.microsoft.com/security-info")
                    time.sleep(3)
            
            log(f"Complete: Done={state['leads_done']} Failed={state['leads_failed']}")
            
        except Exception as e:
            log(f"Critical error: {e}", "ERROR")
        finally:
            if browser:
                browser.close()

def main():
    print("=" * 58)
    print(f"   RK AUTOMATION - CLOUD SHELL")
    print(f"   Worker ID: {WORKER_ID}")
    print(f"   IP: {LOCAL_IP}")
    print("=" * 58)
    
    log("Worker starting...")
    state["current_action"] = "Polling for tasks"
    
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    
    consecutive_errors = 0
    while not state["stop_requested"]:
        if state["paused"]:
            time.sleep(5)
            continue
        
        task = fetch_task()
        if task is None:
            consecutive_errors += 1
            time.sleep(min(60, 10 * consecutive_errors))
            continue
        
        consecutive_errors = 0
        if "account" in task:
            start_automation(task["account"], task["leads"], task["settings"], task.get("campaign"))
            log("Cooldown 30s...")
            time.sleep(30)
        else:
            time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Stopped by user")
        sys.exit(0)
AUTOSCRIPT

# Step 5: Create run script
echo -e "${YELLOW}[5/5] Creating run scripts...${NC}"

cat > ~/rk-automation/run.sh << 'RUNSCRIPT'
#!/bin/bash
cd ~/rk-automation

# Kill existing
pkill -f "cloud_automation.py" 2>/dev/null

# Run with nohup
nohup python3 cloud_automation.py > output.log 2>&1 &

echo "✓ Service started with PID: $!"
echo "✓ Logs: tail -f ~/rk-automation/output.log"
RUNSCRIPT

cat > ~/rk-automation/stop.sh << 'STOPSCRIPT'
#!/bin/bash
pkill -f "cloud_automation.py"
echo "✓ Service stopped"
STOPSCRIPT

cat > ~/rk-automation/logs.sh << 'LOGSCRIPT'
#!/bin/bash
tail -f ~/rk-automation/worker.log
LOGSCRIPT

chmod +x ~/rk-automation/*.sh
chmod +x ~/rk-automation/cloud_automation.py

# Set environment
echo 'export PLAYWRIGHT_BROWSERS_PATH=$HOME/.cache/ms-playwright' >> ~/.bashrc
source ~/.bashrc

echo ""
echo -e "${GREEN}=========================================${NC}"
echo -e "${GREEN}  INSTALLATION COMPLETE!${NC}"
echo -e "${GREEN}=========================================${NC}"
echo ""
echo "Commands to run:"
echo "  cd ~/rk-automation"
echo "  ./run.sh    - Start automation"
echo "  ./stop.sh   - Stop automation"
echo "  ./logs.sh   - View logs"
echo ""

CLOUDSETUP

# Run the setup
bash ~/setup_cloudshell.sh