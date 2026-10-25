# Studio Photo Bot

A Telegram bot that turns one phone photo of a garment into a set of studio
catalogue shots. Built for a small clothing seller who was paying for photo
shoots she could not repeat every time the stock changed.

Live, in daily use: 11 users so far, 516 generations billed, about $15 spent.

![stack](https://img.shields.io/badge/python-3.11-blue) 

## What it does

Send a product photo, then walk a button wizard: model → shot → optional manual
tuning → generated image back as a file (uncompressed).

| Shot | Notes |
|---|---|
| Front / back | Pose, crop, background and lighting selectable |
| Flat lay | Arrangement and camera angle |
| Ghost mannequin | Invisible-mannequin look, angle selectable |
| Outdoor / city | Real street backgrounds, seasonal sets (winter / summer) |
| Recolour | Re-dyes the garment to a colour sample sent as a second photo |

Generation runs on `gpt-image-2` through PiAPI. It is an *edit* model — it takes
the real garment as input and preserves it, instead of inventing a look-alike the
way text-to-image would. The model sits behind one dict entry, so swapping or
adding one is a single change; a second model (Nano Banana 2) lived there until it
stopped being worth its price.

## Engineering notes

The parts I would actually talk about in a review:

- **Access by request.** An unknown user cannot spend money. First `/start`
  sends a request card to the admin, who approves or rejects with one button.
  Approvals and rejections survive restarts; `/users` lists everyone and can
  revoke access.
- **Spend accounting.** Every generation is attributed per user and per model in
  USD (`/profile`) — which is how I could see what each model actually cost and
  drop the one that was not earning it.
- **A daily fuse.** `DAILY_LIMIT` caps generations per day. The budget is
  *reserved* before the API call in a single synchronous function and refunded if
  the call fails or the user cancels — so two users pressing the button at the
  same time cannot both slip past the limit.
- **Crash-safe state.** JSON state is written atomically (temp file + replace),
  so a kill mid-write cannot leave a truncated approvals file.
- **Albums.** Telegram delivers a multi-photo album as separate updates; they are
  collected per `media_group_id` behind a short debounce timer instead of being
  treated as several separate orders.
- **Deterministic output size.** Whatever the model returns, Pillow fits it to
  exactly 1050×1400 (3:4) and draws the watermark, so the shop's catalogue stays
  uniform.
- **Long jobs.** Generation is submit-then-poll against the provider with
  timeouts, a typing indicator while waiting, and errors surfaced to the user in
  plain language rather than a stack trace.

## Running it

```bash
pip install aiogram aiohttp pillow
cp .env.example .env   # fill in the tokens
python3 -u bot.py
```

Deployment is deliberately boring: a Linux box, `start.sh`, and cron — `@reboot`
plus a watchdog every two minutes that restarts the process if it is not running.

## Known limits

- State lives in JSON files, not a database. Fine for one shop and a handful of
  users; the first thing I would change if it grew.
- `MemoryStorage` for FSM, so an in-flight wizard is lost on restart.
- The provider blocks some regions, so the host has to sit outside them.
- `GPT_PRICE` defaults to a placeholder and logs a warning until the real
  per-image price from the provider dashboard is set in `.env`.
