import os
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime

LONDON_COORDINATES = {"latitude":51.5074, "longitude":-0.1278}
BIRMINGHAM_COORDINATES = {"latitude":52.4862, "longitude":-1.8904}

# Calculate movement increments
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude'])
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude'] - LONDON_COORDINATES['longitude'])

# Environment Variables for configuration
KAFKA_BOOSTRAP_SERVERS = os.getenv('KAFKA_BOOTSRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()

if __name__=="__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }
    producer = SerializingProducer(producer_config)