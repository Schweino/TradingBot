"""
Checks the Iberostar booking page for a Junior Suite Ocean Front discount >= 60%.
Sends a Windows toast notification and email if the threshold is met.
Run daily via Windows Task Scheduler.

Note: Uses headed Chrome (non-headless) because the site blocks headless browsers.
A Chrome window will briefly appear and close during each check.
"""

import re
import sys
import subprocess
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
from playwright.sync_api import sync_playwright

# --- Email config -----------------------------------------------------------
GMAIL_SENDER   = "schweino68@gmail.com"   # sending from your own Gmail
GMAIL_APP_PWD  = "nwkcjunikmdwsxtt"
EMAIL_TO       = "schweino68@gmail.com"
# ---------------------------------------------------------------------------

URL = (
    "https://booking.iberostar.com/Reservations/Availability"
    "?adultohab0=2&bebehab0=0&codiconc=294&conccodi=76&cp_tealium=&"
    "edadpersona0_0=30&edadpersona0_1=30&fechafin=20%2F10%2F2026&"
    "fechaini=15%2F10%2F2026&idiocodi=2&idiomercodi=en&monecodi=USD&"
    "ninohab0=0&numerohabitaciones=1&numeropersonas0=2&ok_promo=0&"
    "origen_soporte=IBE&search_origin=hotel"
)
TARGET_PCT = 60
LOG_FILE = r"C:\xampp\htdocs\Claude\iberostar_check.log"


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def toast(title: str, message: str) -> None:
    """Windows 10/11 toast notification via PowerShell."""
    safe_title = title.replace("'", "''")
    safe_msg = message.replace("'", "''")
    ps = f"""
Add-Type -AssemblyName System.Windows.Forms
$n = New-Object System.Windows.Forms.NotifyIcon
$n.Icon = [System.Drawing.SystemIcons]::Information
$n.BalloonTipTitle = '{safe_title}'
$n.BalloonTipText = '{safe_msg}'
$n.Visible = $true
$n.ShowBalloonTip(10000)
Start-Sleep -Seconds 12
$n.Dispose()
"""
    subprocess.Popen(
        ["powershell", "-WindowStyle", "Hidden", "-Command", ps],
        creationflags=subprocess.CREATE_NO_WINDOW,
    )


def send_email(discount: int) -> None:
    body = (
        f"A {discount}% discount has been detected on the Iberostar Junior Suite Ocean Front.\n\n"
        f"Book now:\n{URL}"
    )
    msg = MIMEText(body)
    msg["Subject"] = "Iberostar Discount Detected"
    msg["From"] = GMAIL_SENDER
    msg["To"] = EMAIL_TO

    with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
        smtp.starttls()
        smtp.login(GMAIL_SENDER, GMAIL_APP_PWD)
        smtp.send_message(msg)
    log(f"Email sent to {EMAIL_TO}")


def extract_discount(page_text: str) -> int | None:
    """Return the highest discount percentage found on the page, or None."""
    # Matches: "50% discount", "50% off"
    matches = re.findall(r"(\d+)\s*%\s*(?:discount|off)", page_text, re.IGNORECASE)
    # Matches: "Up to 50%"
    matches += re.findall(r"[Uu]p\s+to\s+(\d+)\s*%", page_text)
    if not matches:
        return None
    return max(int(m) for m in matches)


def check() -> None:
    log("Starting Iberostar discount check...")
    with sync_playwright() as p:
        # Must use headed mode — site blocks headless browsers via Akamai bot detection.
        browser = p.chromium.launch(channel="chrome", headless=False)
        page = browser.new_page()
        try:
            page.goto(URL, wait_until="networkidle", timeout=60_000)
            page.wait_for_timeout(4000)
            text = page.inner_text("body")
        finally:
            browser.close()

    if len(text) < 500:
        log(f"Page returned unexpectedly short content ({len(text)} chars) — possible block.")
        return

    discount = extract_discount(text)
    if discount is None:
        log("Could not find any discount percentage on the page.")
        return

    log(f"Discount found: {discount}%")

    if discount >= TARGET_PCT:
        msg = f"Iberostar Junior Suite Ocean Front is now {discount}% off! Check it now."
        log(f"ALERT: {msg}")
        toast("Iberostar Deal Alert!", msg)
        try:
            send_email(discount)
        except Exception as e:
            log(f"Email failed: {e}")
    else:
        log(f"Discount is {discount}% — below the {TARGET_PCT}% threshold. No action.")


if __name__ == "__main__":
    try:
        check()
    except Exception as exc:
        log(f"ERROR: {exc}")
        sys.exit(1)
