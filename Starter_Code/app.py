# Import the dependencies.
import numpy as np
import datetime as dt
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify



#################################################
# Database Setup
#################################################

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)



#################################################
# Flask Routes
#################################################
@app.route("/")
def home():
    #Return all the available routes
    return(
        f'Available Routes:<br/>'
        f'/api/v1.0/precipitation - dictionary of precipitation by date of last year<br/>'
        f'/api/v1.0/stations - list of stations<br/>'
        f'/api/v1.0/tobs - list of temperature observations for the most active station<br/>'
        f'/api/v1.0/start - min, max and average temperature from start date(YYY-MM-DD format)<br/>'
        f'/api/v1.0/start/end - min, max and average temperature from start date to end date(YYY-MM-DD format)<br/>'
    )
@app.route('/api/v1.0/precipitation')
def precipitation():
    # Calculate the date one year from the last date in data set.
    current_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    previous_year = dt.datetime.strftime(dt.datetime.strptime(current_date, '%Y-%m-%d') - dt.timedelta(days=365), '%Y-%m-%d')
    
    # Perform a query to retrieve the data and precipitation scores
    date_precipitation = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date >= previous_year).\
        order_by(Measurement.date.desc()).all()
    
    # Convert the query result into dictionary format
    precipitation = {}
    for i in date_precipitation:
        if i[0] not in precipitation:
            precipitation[i[0]] = []
        precipitation[i[0]].append(i[1])
    
    print('precipitation query successful')
    session.close()
    return precipitation

@app.route('/api/v1.0/stations')
def stations():
    # Query station id and name
    stations = session.query(Station.station, Station.name).all()
    print('Station query successful')
    session.close()
    return jsonify([(i[0], i[1]) for i in stations])

@app.route('/api/v1.0/tobs')
def tobs():
    # Calculate the date one year from the last date in data set.
    current_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    previous_year = dt.datetime.strftime(dt.datetime.strptime(current_date, '%Y-%m-%d') - dt.timedelta(days=365), '%Y-%m-%d')
    
    # List the stations and their counts in descending order.
    active_stations = session.query(Measurement.station, func.count(Measurement.station)).\
        group_by(Measurement.station).\
        order_by(func.count(Measurement.station).desc()).all()
    
    # Finding temperature of the most active station
    station_name = active_stations[0][0]
    temp = session.query(Measurement.tobs).filter(Measurement.station == station_name).\
        filter(Measurement.date >= previous_year).all()
    
    print('tobs query successful')
    session.close()
    return jsonify([i[0] for i in temp])

@app.route('/api/v1.0/<start>')
def temp_start(start):
    # Verifying if the date is in correct format (YYYY-MM-DD)
    try:
        dt.date.fromisoformat(start)
    except ValueError:
        return jsonify({"error": f'Date must be in YYYY-MM-DD format'})
    
    # Query based on data used
    temp_observations = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), \
        func.avg(Measurement.tobs)).filter(Measurement.date >= start).all()
    
    print('Start Date query successful')
    session.close()
    return jsonify([temp_observations[0][0], temp_observations[0][1], temp_observations[0][2]])

@app.route('/api/v1.0/<start>/<end>')
def temp_start_end(start, end):
    # Verifying if the date is in correct format (YYYY-MM-DD)
    try:
        dt.date.fromisoformat(start)
        dt.date.fromisoformat(end)
        assert start < end
    except ValueError:
        return jsonify({"error": f'Date must be in YYYY-MM-DD format'})
    except AssertionError:
        return jsonify({"error": f'End Date must be later than start date'})
    
    # Query based on data used
    temp_observations = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), \
        func.avg(Measurement.tobs)).filter(Measurement.date >= start).\
        filter(Measurement.date <= end).all()
    
    print('Start-End Date query successful')
    session.close()
    return jsonify([temp_observations[0][0], temp_observations[0][1], temp_observations[0][2]])

if __name__ == '__main__':
    app.run(debug=True)