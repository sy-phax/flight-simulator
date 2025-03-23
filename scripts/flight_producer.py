#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 23 14:43:18 2025

@author: syphaxmedjkane
"""

from kafka import KafkaProducer
import json
import time
import random
from faker import Faker
from kafka import KafkaAdminClient
from datetime import datetime
from botocore.config import Config
import boto3


# Attempt to connect to Kafka server
try:
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")  # Connect to Kafka on localhost at port 9092
    print("Connection to Kafka successful!")  # Confirm successful connection
except Exception as e:
    print("Unable to connect to Kafka. Please check if the broker is running.")  # Error message if connection fails
    print(f"Error: {e}")
    exit(1)  # Exit if connection fails

# Initialization of the Faker instance for generating fake data
fake = Faker()

# Create a KafkaProducer instance to send data to Kafka topic
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Kafka server address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serializing the data to JSON format
)

# Initialize S3 client using boto3 to interact with AWS S3
s3_client = boto3.client(
    's3',
    aws_access_key_id='',  # Replace with your AWS access key
    aws_secret_access_key='',  # Replace with your AWS secret key
    region_name='eu-west-3',  # Specify your AWS region
                  )
    
bucket_name = "flight-simulator-data-414266450797"  # The name of the S3 bucket where data will be stored

# Limit for the number of flights
MAX_FLIGHTS = 50  # Max number of active flights
flights_in_progress = {}  # Dictionary to track active flights

def generate_modification_flight_data(flight_id):
    """
    Updates the flight information using the given flight_id.
    Ensures consistency in status and position changes.
    """
    flight = flights_in_progress[flight_id]  # Retrieve the flight from the dictionary
    
    # 1. Geographic update (latitude and longitude should evolve realistically)
    # Ensure that the flight moves coherently without large jumps in location
    new_latitude = flight["latitude"] + random.uniform(-1, 1)  # Random change in latitude
    new_longitude = flight["longitude"] + random.uniform(-1, 1)  # Random change in longitude
    
    # 2. Update the flight status (ensure consistency in status transitions)
    # The flight must pass through "In Flight" before changing to "Takeoff" or "Landing"
    if flight["status"] == "Landing" and random.random() < 0.2:  # 20% chance to change to "In Flight"
        flight["status"] = "In Flight"
    elif flight["status"] == "Takeoff" and random.random() < 0.2:  # 20% chance to change to "In Flight"
        flight["status"] = "In Flight"
    elif flight["status"] == "In Flight" and random.random() < 0.1:  # 10% chance to change to either "Landing" or "Takeoff"
        flight["status"] = random.choice(["Landing", "Takeoff"])

    # 3. Realistic updates for speed, altitude, and direction
    flight["latitude"] = new_latitude
    flight["longitude"] = new_longitude
    flight["altitude"] = random.randint(1000, 40000)  # Altitude between 1000 and 40000 feet
    flight["speed"] = random.randint(200, 900)  # Speed between 200 and 900 knots
    flight["direction"] = random.randint(0, 360)  # Direction in degrees
    flight["timestamp"] = time.time()  # Timestamp for the data

    return flight

def upload_to_s3(data):
    """
    Uploads the flight data to an S3 bucket.
    """
    file_name = f"flight_data_{int(time.time())}.json"  # Create a unique file name using the current timestamp
    s3_client.put_object(
        Bucket=bucket_name,
        Key=f"data/{file_name}",  # Store the file in the "data" folder in the S3 bucket
        Body=json.dumps(data)  # Convert the data to JSON format and upload it
    )
    print(f"Data uploaded to S3: {file_name}")  # Print confirmation message

if __name__ == "__main__":
    topic_name = "flight_data"  # Kafka topic to send flight data to
    print(f"Sending flight data to Kafka topic '{topic_name}' and S3...")

    # Generate 50 flights initially and add them to the 'flights_in_progress' dictionary
    if not flights_in_progress:
        for _ in range(MAX_FLIGHTS):
            flight_id = fake.uuid4()  # Generate a unique flight ID
            flights_in_progress[flight_id] = {
                "flight_id": flight_id,
                "airline": random.choice(["Air France", "Lufthansa", "Delta", "Emirates", "KLM"]),  # Random airline
                "latitude": round(random.uniform(-90, 90), 6),  # Random latitude between -90 and 90 degrees
                "longitude": round(random.uniform(-180, 180), 6),  # Random longitude between -180 and 180 degrees
                "altitude": random.randint(1000, 40000),  # Random altitude between 1000 and 40000 feet
                "speed": random.randint(200, 900),  # Random speed between 200 and 900 knots
                "direction": random.randint(0, 360),  # Random direction in degrees
                "status": "In Flight",  # All flights start as "In Flight"
                "timestamp": time.time()  # Current timestamp
            }

    # Update these 50 flights on each iteration
    while True:
        for flight_id in list(flights_in_progress.keys()):
            # Update flight data
            flight_data = generate_modification_flight_data(flight_id)

            # Send the flight data to Kafka
            producer.send(topic_name, flight_data)
            print(f"Data sent: {flight_data}")

            # Upload the flight data to S3
            upload_to_s3(flight_data)

        time.sleep(10)  # Wait 10 seconds before the next iteration