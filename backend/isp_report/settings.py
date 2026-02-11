import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env only for local development (Cloud Run sets K_SERVICE)
if not os.getenv('K_SERVICE'):
    load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.getenv('DJANGO_SECRET_KEY', 'dev-secret-for-local-testing')

DEBUG = os.getenv('DJANGO_DEBUG', '1') == '1'
DB_ENGINE = os.getenv('DB_ENGINE', 'sqlite').strip().lower()
DB_DRIVER = os.getenv('DB_DRIVER', 'mysqlclient').strip().lower()

if DB_ENGINE == 'mysql' and DB_DRIVER == 'pymysql':
    import pymysql

    pymysql.install_as_MySQLdb()

def _split_env_list(value, default=None):
    if not value:
        return default or []
    return [v.strip() for v in value.split(',') if v.strip()]

ALLOWED_HOSTS = _split_env_list(os.getenv('ALLOWED_HOSTS'), default=['*'])
CSRF_TRUSTED_ORIGINS = _split_env_list(os.getenv('CSRF_TRUSTED_ORIGINS'), default=[])

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'crispy_forms',
    'maria_cache',
    'reports',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'whitenoise.middleware.WhiteNoiseMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'isp_report.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [BASE_DIR / 'reports' / 'templates'],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'isp_report.wsgi.application'

def _mysql_db(name_env, default_name, user_env='DB_USER', pass_env='DB_PASSWORD'):
    return {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': os.getenv(name_env, default_name),
        'USER': os.getenv(user_env, os.getenv('DB_USER', '')),
        'PASSWORD': os.getenv(pass_env, os.getenv('DB_PASSWORD', '')),
        'HOST': os.getenv('DB_HOST', '127.0.0.1'),
        'PORT': int(os.getenv('DB_PORT', '3306')),
        'OPTIONS': {
            'charset': 'utf8mb4',
        },
    }


if DB_ENGINE == 'mysql':
    DATABASES = {
        'default': _mysql_db('DB_NAME', 'isp_report_main'),
        'cache': _mysql_db('CACHE_DB_NAME', 'isp_report_cache', 'CACHE_DB_USER', 'CACHE_DB_PASSWORD'),
    }
else:
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': BASE_DIR / 'db.sqlite3',
            'OPTIONS': {
                'timeout': 30,
            },
        },
        'cache': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': BASE_DIR / 'db_cache.sqlite3',
            'OPTIONS': {
                'timeout': 30,
            },
        },
    }

DATABASE_ROUTERS = ['isp_report.db_routers.MariaCacheRouter']

AUTH_PASSWORD_VALIDATORS = []

LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'Asia/Kabul'
USE_I18N = True
USE_TZ = True

STATIC_URL = '/static/'
STATIC_ROOT = BASE_DIR / 'staticfiles'
MEDIA_URL = '/media/'
MEDIA_ROOT = BASE_DIR / 'media'
LOGIN_URL = 'login'
LOGIN_REDIRECT_URL = '/'

CRISPY_TEMPLATE_PACK = 'bootstrap4'

SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')
CSRF_COOKIE_SECURE = os.getenv('CSRF_COOKIE_SECURE', '0') == '1'
SESSION_COOKIE_SECURE = os.getenv('SESSION_COOKIE_SECURE', '0') == '1'

STORAGES = {
    'default': {
        'BACKEND': 'django.core.files.storage.FileSystemStorage',
    },
    'staticfiles': {
        'BACKEND': 'whitenoise.storage.CompressedManifestStaticFilesStorage',
    }
}
