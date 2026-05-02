"""
Checks Google Flights for the cheapest Omaha -> Cancun round-trip (Oct 15-20 2026).
Sends a Windows toast notification and email when the lowest price drops to $900 or less.
Run daily via Windows Task Scheduler.

Note: Uses headed Chrome (non-headless) — Google Flights blocks headless browsers.
A Chrome window will briefly appear and close during each check.
"""

import re
import sys
import subprocess
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
from playwright.sync_api import sync_playwright

URL = "https://www.google.com/travel/flights/search?tfs=CBwQAhoxEgoyMDI2LTEwLTE1QABICFAJWA5qDAgCEggvbS8wY2hyeHINCAMSCS9tLzAxcTk4bRoxEgoyMDI2LTEwLTIwQAxID1APWBNqDQgDEgkvbS8wMXE5OG1yDAgCEggvbS8wY2hyeEABQAFIAXABggELCP___________wGYAQE&tfu=EgYIAhAAGAA&hl=en-US&gl=US"

TARGET_PRICE  = 900
GMAIL_SENDER  = "schweino68@gmail.com"
GMAIL_APP_PWD = "nwkcjunikmdwsxtt"
EMAIL_TO      = "schweino68@gmail.com"
LOG_FILE      = r"C:\xampp\htdocs\Claude\flights_check.log"


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def toast(title: str, message: str) -> None:
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


def send_email(price: int) -> None:
    body = (
        f"The cheapest flight has dropped to ${price:,}!\n\n"
        f"Search results:\n{URL}"
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


def extract_lowest_price(page_text: str) -> int | None:
    """Return the lowest flight price in dollars found on the page, or None."""
    # Matches "$1,263" or "$900"
    matches = re.findall(r"\$(\d{1,2},?\d{3}|\d{1,3})\b", page_text)
    if not matches:
        return None
    prices = [int(m.replace(",", "")) for m in matches]
    # Filter out implausibly low values (taxes, fees shown as small numbers)
    prices = [p for p in prices if p >= 100]
    return min(prices) if prices else None


def check() -> None:
    log("Starting flight price check...")
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=False)
        page = browser.new_page()
        try:
            page.goto(URL, wait_until="networkidle", timeout=60_000)
            page.wait_for_timeout(5000)
            text = page.inner_text("body")
        finally:
            browser.close()

    if len(text) < 500:
        log(f"Page returned unexpectedly short content ({len(text)} chars) — possible block.")
        return

    lowest = extract_lowest_price(text)
    if lowest is None:
        log("Could not find any flight prices on the page.")
        return

    log(f"Lowest price found: ${lowest:,}")

    if lowest <= TARGET_PRICE:
        msg = f"Flights to Cancun are now ${lowest:,}! Book now."
        log(f"ALERT: {msg}")
        toast("Flight Deal Alert!", msg)
        try:
            send_email(lowest)
        except Exception as e:
            log(f"Email failed: {e}")
    else:
        log(f"Lowest price is ${lowest:,} — above the ${TARGET_PRICE:,} threshold. No action.")


if __name__ == "__main__":
    try:
        check()
    except Exception as exc:
        log(f"ERROR: {exc}")
        sys.exit(1)
