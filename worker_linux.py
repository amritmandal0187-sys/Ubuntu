"""
Optimized Worker Client for Linux/Ubuntu VPS
Features: Auto-recovery, checkpointing, multi-threading, connection resilience
"""
import os
import sys
import time
import json
import socket
import asyncio
import aiohttp
import threading
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from pathlib import Path
import logging

# Playwright imports
from playwright.async_api import async_playwright, Page, Browser, BrowserContext
import pyotp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.expanduser("~/rkworker.log"), encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class WorkerConfig:
    """Worker configuration"""
    api_url: str = os.getenv("API_URL", "http://13.235.87.209:3100")
    worker_id: str = ""
    heartbeat_interval: int = 10
    task_poll_interval: int = 2
    max_retries: int = 3
    checkpoint_interval: int = 30
    request_timeout: int = 30
    os_type: str = "ubuntu"

    def __post_init__(self):
        worker_id_file = Path(".worker_id")
        if not self.worker_id:
            if worker_id_file.exists():
                self.worker_id = worker_id_file.read_text().strip()
            else:
                hostname = socket.gethostname()
                self.worker_id = f"VPS_{hostname}"
                worker_id_file.write_text(self.worker_id)

CONFIG = WorkerConfig()

# ═══════════════════════════════════════════════════════════════════════════════
# STATE MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class WorkerState:
    """Worker state tracking"""
    status: str = "online"  # online, busy, paused, error
    active_threads: int = 0
    max_threads: int = 5
    current_action: str = "Idle"
    leads_done: int = 0
    leads_failed: int = 0
    current_campaign: Optional[int] = None
    current_task: Optional[str] = None
    checkpoint_data: Optional[Dict] = None
    last_heartbeat: Optional[datetime] = None
    version: str = "4.0.0"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

STATE = WorkerState()
STATE_LOCK = threading.Lock()

# ═══════════════════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def get_local_ip() -> str:
    """Get local IP address"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "127.0.0.1"

def prevent_sleep():
    """Prevent system from sleeping (Linux)"""
    try:
        # Use systemd-inhibit to prevent sleep
        subprocess.Popen(
            ["systemd-inhibit", "--what=sleep:idle", "--who=RKWorker",
             "--why=Automation running", "sleep", "infinity"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        logger.info("Sleep prevention enabled (systemd-inhibit)")
    except:
        logger.warning("Could not enable sleep prevention")

def keep_alive_loop():
    """Keep system awake (Linux no-op loop for thread parity)"""
    while True:
        time.sleep(240)

def install_dependencies():
    """Install required dependencies on Ubuntu"""
    deps = [
        "chromium-browser",
        "chromium-chromedriver",
        "libgbm1",
        "libnss3",
        "libxss1",
        "libasound2",
        "libxtst6",
        "libgtk-3-0",
        "libx11-xcb1"
    ]

    logger.info("Checking dependencies...")
    for dep in deps:
        try:
            subprocess.run(["dpkg", "-l", dep], check=True, capture_output=True)
        except:
            logger.info(f"Installing {dep}...")
            subprocess.run(["apt-get", "update"], capture_output=True)
            subprocess.run(["apt-get", "install", "-y", dep], capture_output=True)

# ═══════════════════════════════════════════════════════════════════════════════
# API CLIENT
# ═══════════════════════════════════════════════════════════════════════════════

class APIClient:
    """Async API client with retry logic"""

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session: Optional[aiohttp.ClientSession] = None
        self._connected = False

    async def connect(self):
        """Initialize session"""
        timeout = aiohttp.ClientTimeout(total=CONFIG.request_timeout)
        self.session = aiohttp.ClientSession(timeout=timeout)
        self._connected = True

    async def disconnect(self):
        """Close session"""
        if self.session:
            await self.session.close()
            self._connected = False

    async def request(self, method: str, endpoint: str, **kwargs) -> Optional[Dict]:
        """Make API request with retry"""
        url = f"{self.base_url}{endpoint}"

        for attempt in range(CONFIG.max_retries):
            try:
                async with self.session.request(method, url, **kwargs) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        text = await response.text()
                        logger.warning(f"API error {response.status}: {text}")
                        return None
            except asyncio.TimeoutError:
                logger.warning(f"Request timeout (attempt {attempt + 1})")
            except Exception as e:
                logger.warning(f"Request error (attempt {attempt + 1}): {e}")

            if attempt < CONFIG.max_retries - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff

        return None

    async def heartbeat(self, state: WorkerState) -> Optional[Dict]:
        """Send heartbeat"""
        data = {
            "worker_id": CONFIG.worker_id,
            "ip_address": get_local_ip(),
            "hostname": socket.gethostname(),
            "status": state.status,
            "active_threads": state.active_threads,
            "max_threads": state.max_threads,
            "current_action": state.current_action,
            "leads_done": state.leads_done,
            "leads_failed": state.leads_failed,
            "version": state.version,
            "os_type": CONFIG.os_type
        }
        return await self.request("POST", "/api/worker/heartbeat", json=data)

    async def request_task(self) -> Optional[Dict]:
        """Request new task"""
        data = {
            "worker_id": CONFIG.worker_id,
            "ip_address": get_local_ip()
        }
        return await self.request("POST", "/api/worker/task/request", json=data)

    async def report_task(self, task_id: str, campaign_id: int, lead_id: int,
                         email: str, status: str, error: str = None) -> bool:
        """Report task completion"""
        data = {
            "task_id": task_id,
            "worker_id": CONFIG.worker_id,
            "campaign_id": campaign_id,
            "lead_id": lead_id,
            "email": email,
            "status": status,
            "error_message": error
        }
        result = await self.request("POST", "/api/worker/task/report", json=data)
        return result is not None

    async def save_checkpoint(self, checkpoint_data: Dict) -> bool:
        """Save checkpoint for recovery"""
        data = {
            "worker_id": CONFIG.worker_id,
            **checkpoint_data
        }
        result = await self.request("POST", "/api/worker/checkpoint/save", json=data)
        return result is not None

# ═══════════════════════════════════════════════════════════════════════════════
# AUTOMATION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class AutomationEngine:
    """Playwright automation engine"""

    def __init__(self):
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.totp: Optional[pyotp.TOTP] = None

    async def start(self, headless: bool = True):
        """Start browser"""
        self.playwright = await async_playwright().start()

        # Linux-specific browser launch options
        browser_options = {
            "headless": headless,
            "args": [
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-accelerated-2d-canvas",
                "--no-first-run",
                "--no-zygote",
                "--single-process",
                "--disable-gpu"
            ]
        }

        self.browser = await self.playwright.chromium.launch(**browser_options)
        self.context = await self.browser.new_context()
        self.page = await self.context.new_page()
        logger.info(f"Browser started (headless={headless})")

    async def stop(self):
        """Stop browser"""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        logger.info("Browser stopped")

    async def login_microsoft(self, email: str, password: str, totp_secret: str, headless: bool = True) -> bool:
        """Login to Microsoft account"""
        try:
            logger.info(f"Logging in as {email}")
            self.totp = pyotp.TOTP(totp_secret)
            totp = self.totp

            async def do_fresh_login():
                logger.warning("TOTP re-requested. Closing browser and doing fresh login...")
                try:
                    await self.stop()
                except:
                    pass
                await self.start(headless=headless)

                logger.info("Fresh login: Navigating to Microsoft Login...")
                await self.page.goto("https://login.microsoftonline.com/", timeout=60000)
                await self.page.fill('input[type="email"]', email)
                await self.page.click('input[type="submit"]')
                await asyncio.sleep(2)

                await self.page.fill('input[type="password"]', password)
                await self.page.click('input[type="submit"]')
                await asyncio.sleep(3)

                logger.info("Fresh login: Submitting TOTP...")
                fresh_code = totp.now()
                await self.page.fill('input[type="tel"]', fresh_code)
                await asyncio.sleep(1)
                await self.page.click('input[type="submit"]')
                await asyncio.sleep(3)

            # First Navigation
            await self.page.goto("https://login.microsoftonline.com/", timeout=60000)
            await self.page.fill('input[type="email"]', email)
            await self.page.click('input[type="submit"]')
            await asyncio.sleep(2)

            await self.page.fill('input[type="password"]', password)
            await self.page.click('input[type="submit"]')
            await asyncio.sleep(3)

            # PARALLEL CHECK: Stay signed in OR 2FA Code
            # Handle 2FA Code if it appears immediately after password
            if await self.page.is_visible('input[placeholder="Code"]') or await self.page.is_visible('input[aria-label="Code"]'):
                logger.info("Intermediate 2FA detected. Submitting code...")
                code = totp.now()
                # Use placeholder or aria-label for robustness
                otp_field = 'input[placeholder="Code"]' if await self.page.is_visible('input[placeholder="Code"]') else 'input[aria-label="Code"]'
                await self.page.fill(otp_field, code)
                await asyncio.sleep(0.5)
                await self.page.click('input[type="submit"]')
                await asyncio.sleep(3)

            # Handle "Stay signed in" prompt
            if await self.page.is_visible('input[id="idSIButton9"]'):
                logger.info("'Stay signed in?' detected")
                await asyncio.sleep(3)

            logger.info("Navigating to Security Info portal...")
            await self.page.goto("https://mysignins.microsoft.com/security-info", timeout=60000)
            await asyncio.sleep(5)

            # Now wait/check for TOTP field if required to access the portal
            if await self.page.is_visible('input[placeholder="Code"]') or await self.page.is_visible('input[aria-label="Code"]') or await self.page.is_visible('input[type="tel"]'):
                logger.info("TOTP verification required for Security Info. Submitting code...")
                code = totp.now()
                otp_field = 'input[placeholder="Code"]' if await self.page.is_visible('input[placeholder="Code"]') else \
                            ('input[aria-label="Code"]' if await self.page.is_visible('input[aria-label="Code"]') else 'input[type="tel"]')
                await self.page.fill(otp_field, code)
                await asyncio.sleep(1)
                await self.page.click('input[type="submit"]')
                await asyncio.sleep(5)

                # Check for "Stay signed in" again if it appears after TOTP
                if await self.page.is_visible('input[id="idSIButton9"]'):
                    logger.info("'Stay signed in?' detected after portal TOTP. Clicking...")
                    await asyncio.sleep(3)

            # Verify we are actually on the security info page by waiting for the 'Add' icon
            try:
                await self.page.wait_for_selector('i[data-icon-name="Add"]', timeout=30000)
            except:
                logger.warning("Could not confirm Security Info page load (Add icon not found).")

            logger.info("Login successful")
            return True

        except Exception as e:
            logger.error(f"Login failed: {e}")
            return False

    async def process_lead(self, email: str) -> tuple[bool, Optional[str]]:
        """Process a single lead"""
        try:
            logger.info(f"Processing lead: {email}")

            # Click Add button
            if not await self.page.is_visible('[data-testid="email-input"]'):
                for attempt in range(2):
                    try:
                        await self.page.wait_for_selector('i[data-icon-name="Add"]', timeout=30000)
                        await self.page.click('i[data-icon-name="Add"]')
                        await asyncio.sleep(2)

                        # Check if re-authentication is required (2FA prompt) after clicking Add
                        if await self.page.is_visible('input[placeholder="Code"]') or await self.page.is_visible('input[aria-label="Code"]'):
                            logger.info("Re-authentication required during lead processing. Submitting code...")
                            if self.totp:
                                code = self.totp.now()
                                otp_field = 'input[placeholder="Code"]' if await self.page.is_visible('input[placeholder="Code"]') else 'input[aria-label="Code"]'
                                await self.page.fill(otp_field, code)
                                await self.page.click('input[type="submit"]')
                                await asyncio.sleep(5)

                                # Handle "Stay signed in" if it appears after 2FA
                                if await self.page.is_visible('input[id="idSIButton9"]'):
                                    await self.page.click('input[id="idSIButton9"]')
                                    await asyncio.sleep(3)

                                # Retry clicking Add button
                                continue

                        # Wait for the picker
                        await self.page.wait_for_selector('[data-testid="authmethod-picker-email"]', timeout=15000)
                        await self.page.click('[data-testid="authmethod-picker-email"]')
                        await asyncio.sleep(1)
                        break
                    except Exception as e:
                        if attempt == 0:
                            logger.warning(f"Add button flow interrupted, retrying attempt 2... Error: {e}")
                            continue
                        else:
                            raise e

            # Enter email
            await self.page.wait_for_selector('[data-testid="email-input"]', timeout=15000)
            await self.page.fill('[data-testid="email-input"]', email)
            await self.page.click('button[data-testid="reskin-step-next-button"]')
            await asyncio.sleep(2)

            # Check for rate limit error or OTP field
            try:
                # Wait for either the OTP input OR the error message bar
                await self.page.wait_for_selector('[data-testid="email-verify-challenge-otp-input"], [data-testid="message-bar-error"]', timeout=30000)
            except:
                pass  # Handle timeout in the next check

            # If error bar is visible, check text
            if await self.page.is_visible('[data-testid="message-bar-error"]'):
                error_text = await self.page.inner_text('[data-testid="message-bar-error"]')
                if "too many times" in error_text.lower():
                    logger.warning(f"limit agaya hai next account par move. Error: {error_text}")
                    return False, "rate_limit"

            # Ensure OTP field is visible before proceeding
            await self.page.wait_for_selector('[data-testid="email-verify-challenge-otp-input"]', timeout=5000)

            # Go back for next lead
            await self.page.click('[data-testid="reskin-step-back-button"]')
            await asyncio.sleep(2)

            logger.info(f"Lead processed successfully: {email}")
            return True, None

        except Exception as e:
            logger.error(f"Lead processing failed: {e}")
            return False, str(e)

# ═══════════════════════════════════════════════════════════════════════════════
# TASK PROCESSOR
# ═══════════════════════════════════════════════════════════════════════════════

class TaskProcessor:
    """Process automation tasks"""

    def __init__(self, api_client: APIClient):
        self.api = api_client
        self._stop_event = threading.Event()

    async def process_task(self, task: Dict) -> bool:
        """Process a complete task"""
        global STATE

        task_id = task["task_id"]
        campaign_id = task["campaign_id"]
        account = task["account"]
        leads = task["leads"]
        settings = task["settings"]

        # Create dedicated engine for this task
        engine = AutomationEngine()

        with STATE_LOCK:
            # Update max_threads if campaign specifies it
            campaign_threads = settings.get("threads", 1)
            if campaign_threads > STATE.max_threads:
                STATE.max_threads = campaign_threads

            if not STATE.current_campaign:
                STATE.current_campaign = campaign_id
            if not STATE.current_task:
                STATE.current_task = task_id

            STATE.status = "busy"
            STATE.active_threads += 1

        success_count = 0
        fail_count = 0

        try:
            # Parse leads array
            parsed_leads = []
            for ld in leads:
                if isinstance(ld, dict):
                    parsed_leads.append(ld)
                else:
                    parsed_leads.append({"id": 0, "email": ld})

            # Start browser
            await engine.start(headless=True)
            # Login
            logged_in = await engine.login_microsoft(
                account["email"],
                account["password"],
                account["totp_secret"]
            )

            if not logged_in:
                logger.error("Failed to login, marking all leads as failed")
                for i, item in enumerate(parsed_leads):
                    await self.api.report_task(task_id, campaign_id, item["id"], item["email"], "failed", "login_failed")
                    fail_count += 1
                return False

            # Process each lead
            for i, item in enumerate(parsed_leads):
                if self._stop_event.is_set():
                    logger.info("Task processing stopped")
                    break

                email = item["email"]
                lead_id = item["id"]

                with STATE_LOCK:
                    STATE.current_action = f"Processing {email} ({i+1}/{len(parsed_leads)})"

                success, error = await engine.process_lead(email)

                if success:
                    await self.api.report_task(task_id, campaign_id, lead_id, email, "success")
                    success_count += 1
                    with STATE_LOCK:
                        STATE.leads_done += 1
                else:
                    await self.api.report_task(task_id, campaign_id, lead_id, email, "failed", error)
                    fail_count += 1
                    with STATE_LOCK:
                        STATE.leads_failed += 1

                    error_msg = str(error)
                    if error_msg == "rate_limit":
                        logger.warning("Rate limit reached, stopping batch")
                        break

                    if "Target page" in error_msg or "closed" in error_msg.lower() or "disconnected" in error_msg.lower():
                        logger.warning("Browser closed or disconnected. Skipping remaining leads to move to next account.")
                        break

                    try:
                        await engine.page.goto("https://mysignins.microsoft.com/security-info")
                        await asyncio.sleep(2)
                    except:
                        pass

                # Save checkpoint periodically
                if i % 3 == 0:
                    await self._save_checkpoint(task_id, campaign_id, parsed_leads[i:])

            return success_count > 0

        except Exception as e:
            logger.error(f"Task processing error: {e}")
            return False

        finally:
            await engine.stop()
            with STATE_LOCK:
                STATE.active_threads -= 1
                if STATE.active_threads <= 0:
                    STATE.status = "online"
                    STATE.current_campaign = None
                    STATE.current_task = None
                    STATE.current_action = "Idle"

    async def _save_checkpoint(self, task_id: str, campaign_id: int, remaining_leads: List[Dict]):
        """Save checkpoint for recovery"""
        import datetime as dt
        checkpoint = {
            "campaign_id": campaign_id,
            "task_id": task_id,
            "lead_ids": [ld.get("id") for ld in remaining_leads],
            "progress": {"remaining_emails": [ld.get("email") for ld in remaining_leads]},
            "saved_at": dt.datetime.now(dt.timezone.utc).isoformat()
        }
        await self.api.save_checkpoint(checkpoint)

    def stop(self):
        """Stop current task"""
        self._stop_event.set()

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN WORKER LOOP
# ═══════════════════════════════════════════════════════════════════════════════

class Worker:
    """Main worker class"""

    def __init__(self):
        self.api = APIClient(CONFIG.api_url)
        self.processor: Optional[TaskProcessor] = None
        self._running = False
        self._paused = False

    async def run(self):
        """Main worker loop"""
        global STATE

        logger.info("=" * 60)
        logger.info(f"RK Digital Linux Worker v{STATE.version}")
        logger.info(f"Worker ID: {CONFIG.worker_id}")
        logger.info(f"API URL: {CONFIG.api_url}")
        logger.info("=" * 60)

        # Prevent sleep
        prevent_sleep()
        threading.Thread(target=keep_alive_loop, daemon=True).start()

        # Connect to API
        await self.api.connect()
        self.processor = TaskProcessor(self.api)
        self._running = True

        # Start heartbeat loop
        asyncio.create_task(self._heartbeat_loop())

        consecutive_errors = 0

        while self._running:
            try:
                if self._paused:
                    await asyncio.sleep(5)
                    continue

                # Check if we can take more work
                with STATE_LOCK:
                    current_active = STATE.active_threads
                    max_allowed = STATE.max_threads

                if current_active >= max_allowed:
                    await asyncio.sleep(1)
                    continue

                # Request task
                response = await self.api.request_task()

                if response and response.get("status") == "ok":
                    task = response.get("task")
                    if task:
                        logger.info(f"Received task with {len(task.get('leads', []))} leads")
                        consecutive_errors = 0

                        # Process task in background
                        asyncio.create_task(self.processor.process_task(task))

                        # Wait a bit before requesting another one to let the thread start
                        await asyncio.sleep(1)
                        continue

                elif response and response.get("status") in ["no_campaign", "no_leads", "no_account"]:
                    # No work available, wait longer
                    logger.debug(f"No work: {response.get('message')}")
                    await asyncio.sleep(CONFIG.task_poll_interval * 2)
                    consecutive_errors = 0

                else:
                    # Error or unexpected response
                    consecutive_errors += 1
                    wait_time = min(60, 5 * consecutive_errors)
                    logger.warning(f"Error getting task, waiting {wait_time}s")
                    await asyncio.sleep(wait_time)

                await asyncio.sleep(CONFIG.task_poll_interval)

            except Exception as e:
                logger.error(f"Main loop error: {e}")
                consecutive_errors += 1
                await asyncio.sleep(min(60, 5 * consecutive_errors))

    async def _heartbeat_loop(self):
        """Send periodic heartbeats"""
        while self._running:
            try:
                with STATE_LOCK:
                    state_copy = WorkerState(**STATE.to_dict())

                response = await self.api.heartbeat(state_copy)

                if response and response.get("command"):
                    await self._handle_command(response["command"])

                STATE.last_heartbeat = datetime.utcnow()

            except Exception as e:
                logger.warning(f"Heartbeat error: {e}")

            await asyncio.sleep(CONFIG.heartbeat_interval)

    async def _handle_command(self, command: str):
        """Handle command from server"""
        global STATE

        logger.info(f"Received command: {command}")

        if command == "pause":
            self._paused = True
            with STATE_LOCK:
                STATE.status = "paused"

        elif command == "resume":
            self._paused = False
            with STATE_LOCK:
                STATE.status = "online"

        elif command == "stop":
            self._running = False
            if self.processor:
                self.processor.stop()

        elif command == "restart":
            logger.info("Restarting worker...")
            self._running = False
            if self.processor:
                self.processor.stop()
            # Linux restart (no CREATE_NO_WINDOW flag)
            subprocess.Popen([sys.executable, __file__])
            sys.exit(0)

        elif command == "update":
            logger.info("Update requested - restarting to fetch new version")
            self._running = False
            subprocess.Popen([sys.executable, __file__])
            sys.exit(0)

# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

async def main():
    """Main entry point"""
    worker = Worker()

    try:
        await worker.run()
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        logger.info("Worker shutting down...")

if __name__ == "__main__":
    asyncio.run(main())
