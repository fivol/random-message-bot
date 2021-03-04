import os

from app.bot import bot, logger, DEBUG, create_tables, app


if __name__ == "__main__":
    logger.debug('Bot start pooling')
    create_tables()
    bot.polling()
