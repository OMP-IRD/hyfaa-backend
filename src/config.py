"""Class-based Flask app configuration."""
from os import environ, path
from dotenv import load_dotenv
import logging

basedir = path.abspath(path.dirname(__file__))
load_dotenv(path.join(basedir, ".env"))

class Config:
    """Base config."""
    SECRET_KEY = environ.get('SECRET_KEY')
    STATIC_FOLDER = 'static'
    TEMPLATES_FOLDER = 'templates'


class ProductionConfig(Config):
    FLASK_ENV = 'production'
    DEBUG = False
    TESTING = False
    LOGGING_LEVEL = logging.WARN
    DATABASE_URI = environ.get('DATABASE_URI')

class StagingConfig(Config):
    FLASK_ENV = 'production'
    DEBUG = False
    TESTING = True
    LOGGING_LEVEL = logging.INFO
    DATABASE_URI = environ.get('DATABASE_URI')


class DevelopmentConfig(Config):
    FLASK_ENV = 'development'
    DEBUG = True
    TESTING = True
    LOGGING_LEVEL = logging.DEBUG
    DATABASE_URI = environ.get('DEV_DATABASE_URI')