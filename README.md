# JSE prices.csv generator (for Google Sheets CSV_URL)

This repo generates a `prices.csv` file daily from the **official Jamaica Stock Exchange Daily Quote Sheet PDF**.

## What you get
A file named `prices.csv` committed to this repo on every run, with this schema:

```csv
symbol,last_price,as_at,source
GK,74.23,February 13, 2026,JSE_DAILY_PDF
```
(If a symbol is missing on the quote sheet, `last_price` will be blank for that symbol.)

## Setup
1. Create a new GitHub repo (public or private).
2. Upload all files from this template.
3. Repo → **Settings → Actions → General → Workflow permissions** → set to **Read and write permissions**.

## Configure symbols
Edit `watchlist.txt` (one symbol per line).

## Run
- Actions tab → **Update prices.csv (JSE Daily Quote PDF)** → **Run workflow**
- Or it runs automatically Mon–Fri after close.

## Use in Google Sheets
- Open `prices.csv` in GitHub
- Click **Raw**
- Copy that URL (`https://raw.githubusercontent.com/.../prices.csv`)
- Paste into your portfolio Google Sheet Settings → `CSV_URL`
