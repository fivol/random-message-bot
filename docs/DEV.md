# Developer notes

## Environment

Copy `.env.example` to `.env` and fill it in. Every variable the code reads:

| Variable | Required | What it is |
|---|---|---|
| `BOT_API_TOKEN` | yes | Telegram bot token from @BotFather |
| `PG_URL` | yes | `postgresql://user:pass@host:5432/db` — parsed into user/pass/host/port/name in `app/config.py` |
| `ALARMER_KEY` | no | Key for [@alarmer_bot](https://t.me/alarmer_bot); used to push crash alerts |
| `ADMIN_KEY` | no | Passphrase sent to the bot in a message to pull usage stats |
| `HOST` | no | Public URL, used when running on webhooks instead of polling |
| `DEBUG` | no | Verbose logging; polling is used in every mode today |

## Running

```bash
python main.py            # creates the tables on start, then polls
docker compose up -d --build
```

## Storage

`app/sql.py` holds every query. Messages live in one pool table; reactions
(spam / like / dislike) are rows against a message, and the delivery query
weights the pool by them, so a message marked spam falls out of rotation and a
liked one is shown more.
