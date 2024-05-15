# from kafka import KafkaProducer, errors
# import cv2
# import time

# def extract_and_send_frames(video_path, kafka_topic, kafka_servers):
#     # Initialize Kafka producer
#     producer = KafkaProducer(
#         bootstrap_servers=kafka_servers,
#         batch_size=16384,
#         linger_ms=5,
#         compression_type='gzip'
#         )

#     # Open the video file
#     cap = cv2.VideoCapture(video_path)
    
#     if not cap.isOpened():
#         print("Error: Could not open video.")
#         return

#     frame_count = 0
#     start_time = time.time()

#     while True:
#         ret, frame = cap.read()
        
#         if not ret:
#             break

#         # Encode frame to JPEG format
#         ret, buffer = cv2.imencode('.jpg', frame)
        
#         if not ret:
#             continue

#         # Send frame to Kafka and check if it's successful
#         try:
#             metadata = producer.send(kafka_topic, buffer.tobytes()).get(timeout=1)
#             if metadata:
#                 print(f"Producing {frame_count} frame")
#                 frame_count += 1
#         except errors.KafkaError as e:
#             print(f"Error sending frame to Kafka: {e}")
        
#     end_time = time.time()
    
#     # Release the video capture object
#     cap.release()
#     producer.flush()
#     producer.close()
    
#     # Calculate FPS
#     total_time = end_time - start_time
#     fps = frame_count / total_time if total_time > 0 else 0
    
#     print(f"Extracted and sent {frame_count} frames in {total_time:.2f} seconds.")
#     print(f"Effective FPS: {fps:.2f}")

# if __name__ == "__main__":

#     kafka_topic = "frames"
#     video_path = "C:\\Users\\soman\\Desktop\\PROJECTS\\kafka-docker-python\\video_01.mp4"
#     kafka_servers= "localhost:9092"

#     extract_and_send_frames(video_path, kafka_topic, kafka_servers)


from kafka import KafkaProducer, errors
import cv2
import time
from threading import Thread, Lock
import queue

class FrameExtractor(Thread):
    def __init__(self, video_path, frame_queue):
        Thread.__init__(self)
        self.cap = cv2.VideoCapture(video_path)
        self.frame_queue = frame_queue
        self.lock = Lock()
        
    def run(self):
        while True:
            ret, frame = self.cap.read()
            if not ret:
                break
            with self.lock:
                self.frame_queue.put(frame)
        self.cap.release()
        self.frame_queue.put(None)  # Signal that extraction is done

class FrameSender(Thread):
    def __init__(self, kafka_topic, kafka_servers, frame_queue):
        Thread.__init__(self)
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            batch_size=16384,
            linger_ms=5,
            compression_type='gzip'
        )
        self.kafka_topic = kafka_topic
        self.frame_queue = frame_queue
        self.lock = Lock()
        
    def run(self):
        frame_count = 0
        start_time = time.time()
        
        while True:
            with self.lock:
                frame = self.frame_queue.get()
            
            if frame is None:
                break
            
            ret, buffer = cv2.imencode('.jpg', frame)
            if ret:
                try:
                    metadata = self.producer.send(self.kafka_topic, buffer.tobytes()).get(timeout=1)
                    if metadata:
                        frame_count += 1
                        print(f"Produced {frame_count} frame")
                except errors.KafkaError as e:
                    print(f"Error sending frame to Kafka: {e}")
        
        end_time = time.time()
        self.producer.flush()
        self.producer.close()
        
        total_time = end_time - start_time
        fps = frame_count / total_time if total_time > 0 else 0
        
        print(f"Extracted and sent {frame_count} frames in {total_time:.2f} seconds.")
        print(f"Effective FPS: {fps:.2f}")

def extract_and_send_frames(video_path, kafka_topic, kafka_servers):
    frame_queue = queue.Queue(maxsize=100)  # Limit the queue size to avoid memory issues

    extractor = FrameExtractor(video_path, frame_queue)
    sender = FrameSender(kafka_topic, kafka_servers, frame_queue)

    extractor.start()
    sender.start()

    extractor.join()
    sender.join()

if __name__ == "__main__":
    kafka_topic = "frames"
    video_path = "C:\\Users\\soman\\Desktop\\PROJECTS\\kafka-docker-python\\video_01.mp4"
    kafka_servers = "localhost:9092"

    extract_and_send_frames(video_path, kafka_topic, kafka_servers)
