# Play Store Scraper — Render Deployment Guide

## What you need
- A free GitHub account: https://github.com
- A free Render account: https://render.com

---

## Step 1 — Upload to GitHub

1. Go to https://github.com/new
2. Name the repo: `playstore-scraper`
3. Set it to **Private**
4. Click **Create repository**
5. On the next page click **uploading an existing file**
6. Drag ALL files from this ZIP into the upload area:
   - `main.py`
   - `scraper.py`
   - `requirements.txt`
   - `render.yaml`
   - the entire `static/` folder (drag the folder itself)
7. Click **Commit changes**

---

## Step 2 — Deploy on Render

1. Go to https://render.com and sign up / log in
2. Click **New +** → **Web Service**
3. Choose **Connect a GitHub repository**
4. Authorise Render and select `playstore-scraper`
5. Render will auto-detect `render.yaml` and fill everything in
6. Click **Create Web Service**
7. Wait ~2 minutes for the build to finish
8. Render gives you a live URL like: `https://playstore-scraper-xxxx.onrender.com`

Open that URL — your app is live.

---

## Notes

- Free tier on Render spins down after 15 minutes of inactivity.
  First load after idle takes ~30 seconds to wake up. This is normal.
- Scrape jobs run in memory — restarting the service clears active jobs.
  Always download your enriched CSV before closing the tab.
