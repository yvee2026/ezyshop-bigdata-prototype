"""Capture dashboard screenshots (each tab) into docs/screenshots/ with headless Chromium.

Runs in the official Playwright container on the compose network, so nothing is installed
on the host:

    make screenshots
"""
import sys
import time

from playwright.sync_api import sync_playwright

URL = sys.argv[1] if len(sys.argv) > 1 else "http://dashboard:8501"
OUT = sys.argv[2] if len(sys.argv) > 2 else "/work/docs/screenshots"
TABS = [("Marketing (live)", "01_marketing_live"), ("Operations", "02_operations"),
        ("Customers", "03_customers"), ("Pipeline", "04_pipeline")]

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={"width": 1440, "height": 1100}, device_scale_factor=2)
    page.goto(URL, wait_until="networkidle")
    page.wait_for_selector("text=EzyShop", timeout=60000)
    time.sleep(6)
    for label, name in TABS:
        page.get_by_role("tab", name=label).click()
        time.sleep(7)  # let Plotly finish rendering
        page.screenshot(path=f"{OUT}/{name}.png", full_page=True)
        print(f"saved {OUT}/{name}.png")
    browser.close()
