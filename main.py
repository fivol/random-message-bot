import os

from app.bot import bot, logger, DEBUG, create_tables, app
from app.config import BOT_API_TOKEN, HOST


if __name__ == "__main__":
    bot.delete_webhook()
    logger.debug('Hello Heroku')
    if DEBUG:
        create_tables()
        bot.polling()
    else:
        create_tables()
        app.run(host="0.0.0.0", debug=DEBUG, port=int(os.environ.get('PORT', 5000)))
        bot.remove_webhook()
        bot.set_webhook(url=HOST + BOT_API_TOKEN)
