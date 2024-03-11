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

class Rides(db.Model):
    __tablename__ = 'dash_table'
    __table_args__ = {'schema':PRODUCTION_SCHEMA}
    user_id = db.Column(db.Integer, nullable = False)
    ride_id = db. Column(db.Text, primary_key=True, nullable = False)
    first_name = db. Column(db.Text, nullable = False)
    last_name = db. Column(db.Text, nullable = False)
    gender = db. Column(db.Text, nullable = False)
    age = db. Column(db.Integer, nullable = False)
    bmi = db. Column(db.Float, nullable = False)
    postcode = db. Column(db.Text, nullable = False)
    account_creation = db. Column(db.Text, nullable = False)
    time = db. Column(db.Text, nullable = False)
    bike_model = db. Column(db.Text, nullable = False)
    resistance_avg = db. Column(db.Float, nullable = False)
    heart_rate_avg = db. Column(db.Float, nullable = False)
    heart_rate_min = db. Column(db.Float, nullable = False)
    heart_rate_max = db. Column(db.Float, nullable = False)
    rpm_avg = db. Column(db.Float, nullable = False)
    rpm_min = db. Column(db.Float, nullable = False)
    rpm_max = db. Column(db.Float, nullable = False)
    power_total = db. Column(db.Float, nullable = False)
    power_avg = db. Column(db.Float, nullable = False)
    power_min = db. Column(db.Float, nullable = False)
    power_max = db. Column(db.Float, nullable = False)
    duration_seconds = db. Column(db.Float, nullable = False)

    def __repr__(self):
        return '<Rides %r>' % self.ride_id

row_dict = lambda r: {c.name: str(getattr(r, c.name)) for c in r.__table__.columns}

# Home
@app.route('/')
def index():
    text = """
    <h2 style='color:#7CC37C;'>Deloton RESTful API :)</h2>
    <h3>Endpoints</h3>
    - /all
        <ul><li>get all rides</li></ul>
    - /ride/:id
        <ul>
            <li>get ride by ride_id</li>
            <li>delete ride by ride_id</li>
        </ul>
    - /rider/:id
        <ul><li>get user by user_id</li></ul>
    - /rider/:id/rides
        <ul><li>get rides by user_id</li></ul>
    - /daily
        <ul><li>get rides in current day</li></ul>
    - /daily?date
        <ul><li>get ride by specific date (dd-mm-yyyy)</li></ul>
    """
    return text, 200

# Get all rides
@app.route('/all', methods=['GET'])
def get_all():
    res = db.session.query(Rides).all()
    rides = [f'Number of Rides: {len(res)}']
    for ride in res:
        rides.append(row_dict(ride))
    return jsonify(rides), 200

# Get/Delete ride by ride_id
@app.route('/ride/<id>', methods=['GET', 'DELETE'])
def get_ride(id):
    if(request.method == 'GET'):
        res = db.session.query(Rides).filter(Rides.ride_id == id).first()
        ride = row_dict(res)
        return jsonify(ride), 200
    else:
        res = db.session.query(Rides).filter(Rides.ride_id == id).first()
        ride = [f'Deleted ride: {id}', row_dict(res)]
        db.session.query(Rides).filter(Rides.ride_id == id).first().delete()
        return jsonify(ride), 200

# Get user by user_id
@app.route('/rider/<id>', methods=['GET'])
def get_user(id):
    res = db.session.query(Rides).filter(Rides.user_id == id).all()
    user = {
        'rides':len(res),
        'user_id':res[0].user_id,
        'first_name':res[0].first_name,
        'last_name':res[0].last_name,
        'gender':res[0].gender,
        'bmi':res[0].bmi,
        'postcode':res[0].postcode,
        'account_creation':res[0].account_creation        
    }
    return jsonify(user), 200

# Get rides by user_id
@app.route('/rider/<id>/rides', methods=['GET'])
def get_user_rides(id):
    res = db.session.query(Rides).filter(Rides.user_id == id).all()
    rides = [f'Number of Rides: {len(res)}']
    for ride in res:
        rides.append(row_dict(ride))
    return jsonify(rides), 200

# Get rides by date
@app.route('/daily', methods=['GET'])
def get_daily_rides():
    if 'date' not in request.args:
        daily = date.strftime(date.today(), '%Y-%m-%d %H:%M:%S')
        res = db.session.query(Rides).filter(Rides.time >= daily).all()
        rides = [f'Number of Rides: {len(res)}']
        for ride in res:
            rides.append(row_dict(ride))
        return jsonify(rides), 200
    else:
        query_date = request.args['date']
        formatted_date = '-'.join(query_date.split('-')[::-1])
        restricted_date_object = datetime.strptime(formatted_date, '%Y-%m-%d') + timedelta(hours=24)
        restricted_date = date.strftime(restricted_date_object, '%Y-%m-%d %H:%M:%S')

        res = db.session.query(Rides).filter(Rides.time >= formatted_date).filter(Rides.time < restricted_date).all()
        rides = [f'Number of Rides: {len(res)}']
        for ride in res:
            rides.append(row_dict(ride))
        return jsonify(rides), 200


# Run App
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)