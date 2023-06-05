from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta
from datetime import datetime, timezone
from flask_migrate import Migrate
import pytz
import csv
import uuid

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:SQLRakesh2022@127.0.0.1:3306/restaurantmonitor'
db = SQLAlchemy(app)
migrate = Migrate(app, db)

# Define the database models
class storetimezone(db.Model):
    store_id = db.Column(db.String(25), primary_key=True)
    timezone_str = db.Column(db.String(100))

class pollingdata(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    store_id = db.Column(db.String(25), db.ForeignKey('storetimezone.store_id'))
    timestamp_utc = db.Column(db.String(30)) 
    status = db.Column(db.String(10))

class businesshours(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    store_id = db.Column(db.String(25), db.ForeignKey('storetimezone.store_id'))
    day_of_week = db.Column(db.Integer)
    start_time_local = db.Column(db.String(8))
    end_time_local = db.Column(db.String(8))

class report(db.Model):
    report_id = db.Column(db.String(50), primary_key=True)
    status = db.Column(db.String(20))
    file_path = db.Column(db.String(100))

# Load data from CSV files and populate the database
def load_data_from_csv():
    # Load store data from timezone.csv
    with app.app_context():
        with open('store_timezone.csv', 'r') as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                store_id, timezone_str = row
                store = storetimezone(store_id=store_id, timezone_str=timezone_str)
                db.session.add(store)

        # Load business hours data from business_hours.csv
        with open('store_business_hours.csv', 'r') as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                store_id, day, start_time_local, end_time_local = row
                business_hours = businesshours(store_id=store_id, day_of_week=day,
                                            start_time_local=start_time_local, end_time_local=end_time_local)
                db.session.add(business_hours)

        with open('store_poll.csv', 'r') as file:
            csv_data = csv.DictReader(file)
            for row in csv_data:
                store_id = row['store_id']
                timestamp_utc = row['timestamp_utc']
                status = row['status']

                # Insert the record into the database
                new_polling_data = pollingdata(store_id=store_id, timestamp_utc=timestamp_utc, status=status)
                db.session.add(new_polling_data)

        db.session.commit()

# Function to generate a unique report ID
def generate_report_id():
    return str(uuid.uuid4())

# API endpoint to trigger report generation
@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    # Logic to generate the report
    current_timestamp = datetime.now()
    report_id = generate_report_id()  # Implement a function to generate a unique report ID

    # Get the max timestamp among all observations
    max_timestamp = pollingdata.query.with_entities(pollingdata.timestamp_utc).order_by(pollingdata.timestamp_utc.desc()).first()[0]

    # Perform the report generation logic
    business_hours = businesshours.query.all()

    # Define report headers
    headers = ['store_id', 'uptime_last_hour(in minutes)', 'uptime_last_day(in hours)',
               'update_last_week(in hours)', 'downtime_last_hour(in minutes)',
               'downtime_last_day(in hours)', 'downtime_last_week(in hours)']
    
    report_data = [headers]

    # Iterate over each store
    stores = storetimezone.query.all()
    for store in stores:
        store_id = store.store_id
        timezone_str = store.timezone_str

        # Convert max timestamp to the store's timezone
        max_timestamp_store_tz = convert_utc_to_timezone(max_timestamp, timezone_str)

        # Calculate the time intervals for the report
        interval_last_hour_start = max_timestamp_store_tz - timedelta(hours=1)
        interval_last_day_start = max_timestamp_store_tz - timedelta(days=1)
        interval_last_week_start = max_timestamp_store_tz - timedelta(weeks=1)

        # Retrieve polling data within each time interval
        polling_data_last_hour = pollingdata.query.filter(pollingdata.store_id == store_id,
                                                          pollingdata.timestamp_utc >= interval_last_hour_start,
                                                          pollingdata.timestamp_utc <= max_timestamp).all()

        polling_data_last_day = pollingdata.query.filter(pollingdata.store_id == store_id,
                                                         pollingdata.timestamp_utc >= interval_last_day_start,
                                                         pollingdata.timestamp_utc <= max_timestamp).all()

        polling_data_last_week = pollingdata.query.filter(pollingdata.store_id == store_id,
                                                          pollingdata.timestamp_utc >= interval_last_week_start,
                                                          pollingdata.timestamp_utc <= max_timestamp).all()

        # Interpolate uptime and downtime based on the available polling data and business hours
        uptime_last_hour = interpolate_uptime(polling_data_last_hour, business_hours)
        uptime_last_day = interpolate_uptime(polling_data_last_day, business_hours)
        uptime_last_week = interpolate_uptime(polling_data_last_week, business_hours)

        downtime_last_hour = interpolate_downtime(polling_data_last_hour, business_hours)
        downtime_last_day = interpolate_downtime(polling_data_last_day, business_hours)
        downtime_last_week = interpolate_downtime(polling_data_last_week, business_hours)

        # Create a row for the store in the report data
        row = [store_id, uptime_last_hour, uptime_last_day, uptime_last_week,
               downtime_last_hour, downtime_last_day, downtime_last_week]
        report_data.append(row)

    # Save the report data to a CSV file
    file_path = f'D:/{report_id}.csv'
    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(report_data)

    # Update the report status and file path
    new_report = report(report_id=report_id, status='Complete', file_path=file_path)
    db.session.add(new_report)
    db.session.commit()

    return jsonify({'report_id': report_id})

# API endpoint to retrieve the report status or the CSV file
@app.route('/get_report', methods=['GET'])
def get_report():
    # Get the report ID from the request
    report_id = request.args.get('report_id')

    # Check if the report ID exists in the database
    report_item = report.query.get(report_id)
    if not report_item:
        return jsonify({'status': 'Report not found'})

    # Check the status of the report
    if report_item.status == 'Running':
        return jsonify({'status': 'Running'})
    elif report_item.status == 'Complete':
        # Return the CSV file with the schema described
        file_path = report_item.file_path
        with open(file_path, 'r') as file:
            csv_data = file.read()

        # Return the CSV file and status
        return jsonify({'status': 'Complete', 'report_csv': csv_data})

    return jsonify({'status': 'Unknown error'})

def convert_utc_to_timezone(utc_timestamp, timezone_str):
    utc = pytz.timezone('UTC')
    target_timezone = pytz.timezone(timezone_str)
    utc_time = utc.localize(utc_timestamp)
    target_time = utc_time.astimezone(target_timezone)
    return target_time

# Utility function to interpolate uptime based on polling data and business hours
def interpolate_uptime(polling_data, business_hours):
    total_uptime = 0
    for data in polling_data:
        timestamp = datetime.strptime(data.timestamp_utc, '%Y-%m-%d %H:%M:%S')
        day_of_week = timestamp.weekday()

        # Find the corresponding business hours for the day of the week
        hours = next((hours for hours in business_hours if hours.store_id == data.store_id and hours.day_of_week == day_of_week), None)

        if hours:
            start_time = datetime.strptime(hours.start_time_local, '%H:%M:%S').time()
            end_time = datetime.strptime(hours.end_time_local, '%H:%M:%S').time()

            # Calculate the uptime based on the polling status and business hours
            if start_time <= timestamp.time() <= end_time and data.status == 'open':
                total_uptime += 1

    return total_uptime
# Utility function to interpolate downtime based on polling data and business hours
def interpolate_downtime(polling_data, business_hours):
    total_downtime = 0
    for data in polling_data:
        timestamp = datetime.strptime(data.timestamp_utc, '%Y-%m-%d %H:%M:%S')
        day_of_week = timestamp.weekday()

        # Find the corresponding business hours for the day of the week
        hours = next((hours for hours in business_hours if hours.store_id == data.store_id and hours.day_of_week == day_of_week), None)

        if hours:
            start_time = datetime.strptime(hours.start_time_local, '%H:%M:%S').time()
            end_time = datetime.strptime(hours.end_time_local, '%H:%M:%S').time()

            # Calculate the downtime based on the polling status and business hours
            if start_time <= timestamp.time() <= end_time and data.status == 'closed':
                total_downtime += 1

    return total_downtime


if __name__ == '__main__':
    load_data_from_csv()
    app.run(debug=True)
