import dotenv
import re
import os
dotenv.load_dotenv()

PG_URL = os.environ.get('PG_URL')

if not PG_URL:
    print('INVALID .ENV VARIABLES')
    exit(1)

DB_USER = re.search(r'//(\w+):', PG_URL).group(1)
DB_PASS = re.search(r':(\w+)@', PG_URL).group(1)
DB_HOST = re.search(r'@(\S+):', PG_URL).group(1)
DB_PORT = re.search(r':(\d+)/', PG_URL).group(1)
DB_NAME = re.search(r'/([a-z-]+)$', PG_URL).group(1)

BOT_API_TOKEN = os.getenv('BOT_API_TOKEN')
ALARMER_KEY = os.getenv('ALARMER_KEY', '104839-00cc49-791ee0')
BOT_ID = int(BOT_API_TOKEN.split(':')[0])


DEBUG = os.getenv('DEBUG', False)
if 'DYNO' in os.environ:
    DEBUG = False
HOST = os.getenv('HOST')
ADMIN_KEY = '610'
