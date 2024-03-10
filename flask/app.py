from datetime import date, datetime, timedelta
import json
import os
import urllib.parse

from dotenv import load_dotenv
from flask_sqlalchemy import SQLAlchemy
from flask import Flask, jsonify, request, make_response
from flask_cors import CORS
import psycopg2
from sqlalchemy import desc


# Credentials
load_dotenv()
REGION = os.getenv('REGION')

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = urllib.parse.quote_plus(os.getenv('DB_PASSWORD'))
DB_NAME = os.getenv('DB_NAME')

PRODUCTION_SCHEMA = 'zuckerberg_production'

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
CORS(app, origins=["http://127.0.0.1:8080", "localhost"], supports_credentials=True)