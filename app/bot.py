import telebot
from telebot import types
from peewee import *

from app.config import *
from app.constants import *
from time import time
from flask import Flask, request
from datetime import datetime
import logging
import requests


logger = logging.getLogger('my_logger')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


like_smile = '❤️ᅠ'
dislike_smile = '👎ᅠ'
complain = 'Пожаловаться на содержание'
spam = 'Спам'

reaction_code = {
    complain: 1,
    spam: 2,
    dislike_smile: 3,
    like_smile: 4
}


db = PostgresqlDatabase(DB_NAME, user=DB_USER, password=DB_PASS,
                        host=DB_HOST, port=DB_PORT, autorollback=True)


class BaseModel(Model):
    class Meta:
        database = db


class UserModel(BaseModel):
    chat = IntegerField(unique=True)
    time = DateTimeField(default=datetime.now)
    username = CharField(30, null=True)
    name = CharField(30, null=True)
    photos = CharField(330, null=True)


class ContentModel(BaseModel):
    user = ForeignKeyField(UserModel)
    reply = ForeignKeyField('self', null=True)
    size = IntegerField()
    type = CharField(20, index=True)
    time = DateTimeField(index=True, default=datetime.now)
    text = TextField(null=True)
    media = CharField(200, null=True)
    message_id = IntegerField()


class ShowsModel(BaseModel):
    time = DateTimeField(default=datetime.now, index=True)
    content = ForeignKeyField(ContentModel)
    user = ForeignKeyField(UserModel)
    message_id = IntegerField()
    reaction = IntegerField(null=True)


def create_tables():
    with db:
        db.create_tables([UserModel, ContentModel, ShowsModel])


bot = telebot.TeleBot(BOT_API_TOKEN)
app = Flask(__name__)

remove_markup = telebot.types.ReplyKeyboardRemove(selective=True)


def send_message(chat, message):
    bot.send_message(chat, str(message), reply_markup=remove_markup)


def alarm(message):
    try:
        url = f'https://alarmerbot.ru/?key={ALARMER_KEY}&message={message}'
        requests.get(url)
    except:
        logger.exception('ALARMER ERROR')


def try_catch(error_code):
    def decorator(func):
        def wrapper(*args, **kwargs):
            if DEBUG:
                return func(*args, **kwargs)
            try:
                return func(*args, **kwargs)
            except:
                logger.exception('ERROR %s', error_code)
                send_message(args[0].chat.id,
                             'Извините, что-то пошло не так: на сервере произошла ошибка ' + str(error_code) + ' :(')

        return wrapper

    return decorator


def get_top_message_for_user(chat, seconds_ago):
    from datetime import datetime
    user = UserModel.select().where(UserModel.chat == chat)
    user_shows = ShowsModel.select(ShowsModel.content).distinct().where(ShowsModel.user == user)
    if not seconds_ago:
        time_from = datetime.fromtimestamp(1)
    else:
        time_from = datetime.fromtimestamp(datetime.now().timestamp() - seconds_ago)
    content = (ContentModel.select(ContentModel.id).
               where((ContentModel.type != 'text') &
                     (ContentModel.user != user) &
                     (ContentModel.reply.is_null()) &
                     (ContentModel.id.not_in(user_shows)) &
                     (ContentModel.time > time_from))).alias('content')
    shows = (ShowsModel.select(ShowsModel.id, ShowsModel.content, ShowsModel.reaction).where(
        ShowsModel.content.in_(content)))

    count = fn.COUNT(ShowsModel.id).alias('count')
    reactions = (shows.select(ShowsModel.content, ShowsModel.reaction, count).
                 group_by(ShowsModel.content, ShowsModel.reaction))

    rnull_shows = (reactions.select(ShowsModel.content, count).where(ShowsModel.reaction.is_null())).alias('r_null')
    r1_shows = (reactions.select(ShowsModel.content, count).where(ShowsModel.reaction == 1)).alias('r1')
    r2_shows = (reactions.select(ShowsModel.content, count).where(ShowsModel.reaction == 2)).alias('r2')
    r3_shows = (reactions.select(ShowsModel.content, count).where(ShowsModel.reaction == 3)).alias('r3')
    r4_shows = (reactions.select(ShowsModel.content, count).where(ShowsModel.reaction == 4)).alias('r4')
    content_values = (
        ContentModel.select(ContentModel.id,
                            fn.COALESCE(r1_shows.c.count, 0).alias('r1_count'),
                            fn.COALESCE(r2_shows.c.count, 0).alias('r2_count'),
                            fn.COALESCE(r3_shows.c.count, 0).alias('r3_count'),
                            fn.COALESCE(r4_shows.c.count, 0).alias('r4_count'),
                            fn.COALESCE(rnull_shows.c.count, 0).alias('rnull_count')).
            join(content, on=(content.c.id == ContentModel.id)).
            join(r1_shows, JOIN.FULL_OUTER, on=(ContentModel.id == r1_shows.c.content_id)).
            join(r2_shows, JOIN.FULL_OUTER, on=(ContentModel.id == r2_shows.c.content_id)).
            join(r3_shows, JOIN.FULL_OUTER, on=(ContentModel.id == r3_shows.c.content_id)).
            join(r4_shows, JOIN.FULL_OUTER, on=(ContentModel.id == r4_shows.c.content_id)).
            join(rnull_shows, JOIN.FULL_OUTER, on=(ContentModel.id == rnull_shows.c.content_id))
    ).alias('content_values')

    metric = (
            content_values.c.r1_count * (-5) +
            content_values.c.r2_count * (-3) +
            content_values.c.r3_count * (-1) +
            content_values.c.r4_count * (+1)
    )
    top_row = (ContentModel.select(ContentModel, metric.alias('metric')).
               join(content_values, on=(content_values.c.id == ContentModel.id)).
               order_by(-metric)).first()

    return top_row


@bot.message_handler(commands=['start', 'help'])
@try_catch(81)
def start_command(message):
    name = f'{message.from_user.first_name}_{message.from_user.last_name}'[:30]
    username = message.from_user.username
    photos = ''
    if isinstance(username, str):
        username = username[:30]
    try:
        photos = bot.get_user_profile_photos(message.chat.id)
        photos = photos.photos[:10]
        photos = [photo[-1].file_id for photo in photos]
        photos = ' '.join(photos)[:330]
    except:
        logger.exception('Fail to get user photos')

    chat = message.chat.id
    if message.text == '/start':
        row = UserModel.insert(chat=message.chat.id, name=name,
                               username=username, photos=photos).on_conflict_ignore().execute()
        if not row:
            send_message(chat, 'Привет еще раз!\nТы уже зарегистрирован.')
        else:
            send_message(chat, welcome)
            send_message(chat, rules)
            send_message(chat, support)
            send_message(chat, wishes)
            send_message(chat, examples)
            send_message(chat, 'Удачи))')
    else:
        send_message(chat, rules)
        send_message(chat, wishes)
        send_message(chat, support)
        send_message(chat, examples)


def send_content(chat, row, text=None):
    if not row:
        bot.send_message(chat, 'NULL')
        return

    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False, selective=True)
    spam_button = types.KeyboardButton(spam)
    content = types.KeyboardButton(complain)
    bad = types.KeyboardButton(dislike_smile)
    good = types.KeyboardButton(like_smile)
    markup.row(content)
    markup.row(spam_button, bad, good)
    markup.row(types.KeyboardButton('/get'))

    if row.text:
        text = row.text

    reply_id = None
    if row.reply:
        reply_id = row.reply.message_id
    if row.type == 'voice':
        mess = bot.send_voice(chat, row.media, caption=text, reply_to_message_id=reply_id, reply_markup=markup)
    elif row.type == 'video_note':
        mess = bot.send_video_note(chat, row.media, reply_to_message_id=reply_id, reply_markup=markup)
    elif row.type == 'photo':
        mess = bot.send_photo(chat, row.media, caption=text, reply_to_message_id=reply_id, reply_markup=markup)
    elif row.type == 'text':
        mess = bot.send_message(chat, text, reply_to_message_id=reply_id, reply_markup=markup)
    else:
        send_message(chat, 'Кажется, тебе не повезло, на сервере ошибка 54 :(')
        logger.error('UNEXPECTED CONTENT TYPE')
        return
    user = UserModel.select().where(UserModel.chat == chat)
    ShowsModel.insert(user=user, content=row, message_id=mess.message_id).on_conflict_ignore().execute()


@bot.message_handler(commands=['get'])
@try_catch(23)
def get_message(message):
    t0 = time()
    chat = message.chat.id
    user = UserModel.select().where(UserModel.chat == chat)
    try:
        content_sent = (ContentModel.select(fn.COUNT(ContentModel.id).alias('count')).
                        where((ContentModel.user == user) &
                              (ContentModel.reply.is_null())))
        content_shows = (ShowsModel.select(fn.COUNT(ShowsModel.content).alias('count')).join(ContentModel).
                         where((ShowsModel.user == user) &
                               (ShowsModel.content.reply.is_null())))
        query = content_sent | content_shows
        result = query.dicts().execute()
        content_sent_count = result[0]['count']
        content_shows_count = result[1]['count']

        a1 = 3
        d = 2
        can_be_shown = round((content_sent_count / 2) * (2 * a1 + (content_sent_count - 1) * d))
        logger.info('chat: %s, sent: %s, received: %s, can_receive: %s',
                    chat,
                    content_sent_count,
                    content_shows_count,
                    can_be_shown)
        if round(can_be_shown) == content_shows_count:
            send_message(chat, 'Для того, чтобы получить сообщение, отправьте свое')
            return
    except:
        logger.exception('Fail to calculate can user get message')

    logger.debug('Check messages sum time: %s', time() - t0)
    t0 = time()

    user_shows = ShowsModel.select(ShowsModel.content).distinct().where(ShowsModel.user == user)
    content = (ContentModel.select(ContentModel.id).
               where((ContentModel.type != 'text') &
                     (ContentModel.id.not_in(user_shows) &
                      (ContentModel.reply.is_null())) &
                     (ContentModel.user != user)).
               order_by(-ContentModel.time).
               limit(500)).alias('content')
    shows = (ShowsModel.select(ShowsModel.id, ShowsModel.content, ShowsModel.reaction).where(
        ShowsModel.content.in_(content)))

    count = fn.COUNT(ShowsModel.id).alias('count')
    reactions = (shows.select(ShowsModel.content, ShowsModel.reaction, count).
                 group_by(ShowsModel.content, ShowsModel.reaction))

    rnull_shows = (reactions.select(ShowsModel.content, count).where(ShowsModel.reaction.is_null())).alias('r_null')
    r1_shows = (reactions.select(ShowsModel.content, count).where(ShowsModel.reaction == 1)).alias('r1')
    r2_shows = (reactions.select(ShowsModel.content, count).where(ShowsModel.reaction == 2)).alias('r2')
    r3_shows = (reactions.select(ShowsModel.content, count).where(ShowsModel.reaction == 3)).alias('r3')
    r4_shows = (reactions.select(ShowsModel.content, count).where(ShowsModel.reaction == 4)).alias('r4')
    content_values = (
        ContentModel.select(ContentModel.id,
                            fn.COALESCE(r1_shows.c.count, 0).alias('r1_count'),
                            fn.COALESCE(r2_shows.c.count, 0).alias('r2_count'),
                            fn.COALESCE(r3_shows.c.count, 0).alias('r3_count'),
                            fn.COALESCE(r4_shows.c.count, 0).alias('r4_count'),
                            fn.COALESCE(rnull_shows.c.count, 0).alias('rnull_count')).
            join(content, on=(content.c.id == ContentModel.id)).
            join(r1_shows, JOIN.FULL_OUTER, on=(ContentModel.id == r1_shows.c.content_id)).
            join(r2_shows, JOIN.FULL_OUTER, on=(ContentModel.id == r2_shows.c.content_id)).
            join(r3_shows, JOIN.FULL_OUTER, on=(ContentModel.id == r3_shows.c.content_id)).
            join(r4_shows, JOIN.FULL_OUTER, on=(ContentModel.id == r4_shows.c.content_id)).
            join(rnull_shows, JOIN.FULL_OUTER, on=(ContentModel.id == rnull_shows.c.content_id))
    ).alias('content_values')

    metric = (
            content_values.c.r1_count * (-5) +
            content_values.c.r2_count * (-3) +
            content_values.c.r3_count * (-1) +
            content_values.c.r4_count * (+1)
    )
    top_metric = (ContentModel.select(ContentModel.id, metric.alias('metric')).
                  join(content_values, on=(content_values.c.id == ContentModel.id)).
                  order_by(-metric).
                  limit(100))

    query = (ContentModel.select().
             join(top_metric, on=(top_metric.c.id == ContentModel.id)).
             order_by(fn.Random()))

    row = query.first()
    logger.debug('Get row to show time: %s', time() - t0)

    if not row:
        send_message(chat, 'База сообщений пока пуста или ты все уже посмотрел. \nМожешь отправить еще!')
    else:
        send_content(chat, row)


@bot.message_handler(content_types=['voice', 'photo', 'text', 'video_note'])
@try_catch(12)
def input_content(message):
    content_type = message.content_type
    logger.debug('MESSAGE %s', content_type)
    chat = message.chat.id
    is_review = False
    review = None
    reply_content = None
    reply_author = None
    if message.content_type == 'text':
        texts = message.text.split(' ')[1:]
        if message.text.startswith('/media'):
            if len(texts) == 2:
                command, value = texts
                if command == 'photo':
                    bot.send_photo(chat, value)
                    return
                elif command == 'voice':
                    bot.send_voice(chat, value)
                    return
                elif command == 'video_note':
                    bot.send_video_note(chat, value)
                    return
        elif message.text.startswith('/admin'):
            if len(texts) >= 2:
                if texts[0] == ADMIN_KEY:
                    texts = texts[1:]
                    if len(texts) == 1:
                        command = texts[0]
                        if command == 'shows':
                            send_message(chat, ShowsModel.select().join(ContentModel).
                                         where(ShowsModel.content.reply.is_null()).count())
                        elif command == 'content':
                            send_message(chat, ContentModel.select().where(ContentModel.reply.is_null()).count())
                        elif command == 'reply':
                            send_message(chat, ContentModel.select().where(ContentModel.reply.is_null(False)).count())
                        elif command == 'user':
                            send_message(chat, UserModel.select().count())
                        elif command == 'top':
                            row = get_top_message_for_user(chat, None)
                            send_content(chat, row)
                            if row:
                                send_message(chat, 'Metric value: ' + str(row.metric))

                    elif len(texts) == 2:
                        command, value = texts
                        if command == 'top':
                            row = get_top_message_for_user(chat, int(value) * (3600 * 24))
                            send_content(chat, row)
                            if row:
                                send_message(chat, 'Metric value: ' + str(row.metric))

                    if len(texts) >= 2:
                        if texts[0] == 'all':
                            value = message.text.split(f' all ')[1]
                            for user in UserModel.select():
                                try:
                                    send_message(user.chat, value)
                                except:
                                    logger.exception('Fail to send message to user %s %s', chat, value)
                        if texts[0] == 'sql':
                            value = message.text.split(f' sql ')[1]
                            break_words = ['delete', 'set', 'update', 'remove', 'alter',
                                           'create', 'table', 'drop', 'cascade', 'insert']
                            sql = value.lower()
                            good_sql = True
                            for word in break_words:
                                if word in sql:
                                    good_sql = False
                                    break
                            if good_sql:
                                result = db.execute_sql(value)
                                bot.send_message(chat, str(result.fetchall()))
                    return
                else:
                    alarm('RANDOM MESSAGE BOT\n try to user admin with wrong key' + message.text)
        elif '/support' in message.text:
            review = message.text.strip('/support ').strip()
            if not review:
                send_message(chat,
                             'Если вы хотите написать в поддержку, наберите /support и текст в этом же сообщении')
                return

            name = f'{message.from_user.first_name} {message.from_user.last_name}'
            full_name = f'name: {name}\nusername: {message.from_user.username}\nid: {chat}'
            review_text = f'Отзыв от\n{full_name}\n' + review
            alarm('ОТЗЫВ ИЗ БОТА (RandomMessageBot)\n' + review_text)
            send_message(chat, 'Спасибо за отзыв!')
            return

    media_id = None

    def get_media_id_size(mess):
        if mess.content_type == 'photo':
            return mess.photo[-1].file_id, mess.photo[-1].file_size
        if mess.content_type == 'voice':
            return mess.voice.file_id, mess.voice.file_size
        if mess.content_type == 'video_note':
            return mess.video_note.file_id, mess.video_note.file_size

    if content_type == 'text':
        size = len(message.text)
    else:
        media_id, size = get_media_id_size(message)
    if not is_review:
        if message.reply_to_message:
            reply = message.reply_to_message

            if reply.from_user.id == BOT_ID:
                reply_id = reply.message_id
                reply_shows = ShowsModel.select(). \
                    join(ContentModel). \
                    where(ShowsModel.message_id == reply_id).first()
                if not reply_shows:
                    send_message(message.chat.id,
                                 'Это техническое сообщение, его нельзя пересылать.')
                    return
                else:

                    reply_author = reply_shows.content.user.chat
                    reply_content = reply_shows.content

                    if message.text:
                        if 'wi' in message.text and ADMIN_KEY in message.text:
                            user = reply_content.user
                            name = f'id: {user.id}\nchat: {reply_author}\nname: {user.name}\nusername: {user.username}'
                            send_message(chat, name)
                            return
            else:
                send_message(message.chat.id,
                             'Пересылать можно только сообщения от бота.\nТвой ответ будет отправлен его автору.')
                return
        elif content_type == 'text':
            if message.text in reaction_code:
                logger.debug('REACTION')
                if message.text == complain:
                    send_message(message.chat.id, 'Спасибо, что следите за контентом!')
                code = reaction_code[message.text]
                with db.atomic():
                    show = ShowsModel.select(). \
                        where(ShowsModel.user == UserModel.get(chat=message.chat.id)). \
                        order_by(-ShowsModel.time).first()
                    if show:
                        show.reaction = code
                        show.save()
                get_message(message)
            else:
                send_message(message.chat.id, 'Текстовые сообщения можно отправлять только в ответ на другие.')
            return

    text = None
    if content_type == 'text':
        text = message.text
    elif message.caption:
        text = message.caption

    if not reply_author:
        if not text:
            if ContentModel.select().where((ContentModel.media == media_id) & (ContentModel.text.is_null())).count():
                send_message(message.chat.id, "Такой контент уже существует.\nПовторная отправка запрещена!")
                return
        elif ContentModel.select().where((ContentModel.media == media_id) & (ContentModel.text == text)).count():
            send_message(message.chat.id,
                         "Такой контент (c этой же надписью) уже существует.\nПовторная отправка запрещена!")
            return

    row = ContentModel.insert(
        media=media_id,
        text=text,
        type=content_type,
        user=UserModel.select(UserModel.id).where(UserModel.chat == message.chat.id),
        size=size,
        message_id=message.message_id,
        reply=reply_content
    ).on_conflict_ignore().execute()

    if row is None:
        send_message(message.chat.id, "Ваше сообщение по непонятной причине не добавлено...")
    else:
        if reply_author:
            send_content(reply_author, ContentModel.get_by_id(row))
            send_message(message.chat.id, 'Ответ отправлен.')
        else:
            send_message(message.chat.id, 'Сообщение успешно зарегистрировано!')


@app.route('/', methods=['POST'])
def get_message():
    bot.process_new_updates([telebot.types.Update.de_json(request.stream.read().decode("utf-8"))])
    return "!", 200


@app.route('/stop')
def webhook_remove():
    bot.remove_webhook()
    return 'WEBHOOKS STOPPED', 200


@app.route("/start")
def webhook():
    bot.remove_webhook()
    bot.set_webhook(url=HOST)
    return 'WEBHOOKS STARTED', 200
