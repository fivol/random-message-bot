# random-message-bot

A Telegram bot for anonymous exchange between strangers. You send a message —
text, voice, photo, video note — it goes into a shared pool and reaches someone
else. They can reply to it, you can reply back, and neither of you learns who
the other is.

Live bot: [@random_message_bot](https://t.me/random_message_bot).
The bot speaks Russian; the code and docs are in English.
The original product description (RU) is kept at [docs/README.ru.md](docs/README.ru.md).

## Features

- **Anonymous threads** — swipe-reply to a received message to answer its
  author; the reply itself can be answered, so a whole conversation can happen
  without either side having a handle to the other.
- **Any content** — voice messages, photos, video notes, text.
- **Feedback loop** — every delivered message can be marked spam / liked /
  disliked. Spam drops its delivery odds sharply; likes and dislikes tune what
  each person is shown next.
- **Commands** — `/start`, `/help`, `/get` (pull a random message), and
  `/support` to reach the author.

## Layout

```
main.py            # entry point: creates tables, starts polling
app/
  bot.py           # handlers, delivery and reaction logic (pyTelegramBotAPI)
  sql.py           # PostgreSQL queries: pool, reactions, ranking
  config.py        # environment parsing
  constants.py
docs/DEV.md        # developer notes
```

## Run it

```bash
cp .env.example .env      # fill in BOT_API_TOKEN and PG_URL
pip install -r requirements.txt
python main.py
```

With Docker:

```bash
cp .env.example .env
docker compose up -d --build
```

## Configuration

| Variable | Required | What it is |
|---|---|---|
| `BOT_API_TOKEN` | yes | Telegram bot token from [@BotFather](https://t.me/BotFather) |
| `PG_URL` | yes | `postgresql://user:pass@host:5432/db` |
| `ALARMER_KEY` | no | Key for [@alarmer_bot](https://t.me/alarmer_bot) crash alerts |
| `ADMIN_KEY` | no | Passphrase that unlocks usage stats over chat |
| `HOST` | no | Public URL, for webhook mode |
| `DEBUG` | no | Verbose logging |

Full developer notes: [docs/DEV.md](docs/DEV.md).

## License

MIT — see [LICENSE](LICENSE).
