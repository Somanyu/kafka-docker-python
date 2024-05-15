import cv2
from kafka import KafkaConsumer
import os
import numpy as np

def consume_and_save_frames(kafka_topic, kafka_servers, output_folder):
    # Create output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Initialize Kafka consumer
    consumer = KafkaConsumer(kafka_topic, bootstrap_servers=kafka_servers)

    frame_count = 0

    for message in consumer:
        frame = cv2.imdecode(np.frombuffer(message.value, np.uint8), cv2.IMREAD_COLOR)

        # Save frame as an image
        frame_filename = os.path.join(output_folder, f"frame_{message.offset}.jpg")
        cv2.imwrite(frame_filename, frame)

        print(f"Consuming {frame_count} frame")
        frame_count += 1

    # Close Kafka consumer
    consumer.close()

if __name__ == "__main__":
    kafka_topic = 'frames'
    kafka_servers = 'localhost:9092'
    output_folder = 'frames'

    consume_and_save_frames(kafka_topic, kafka_servers, output_folder)
