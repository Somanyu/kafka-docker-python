from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
import time

# Kafka server configuration
KAFKA_BROKER_URL = 'localhost:9092'
TOPIC_NAME = 'frames'
NUM_PARTITIONS = 1
REPLICATION_FACTOR = 1

# Create an Admin client
admin_client = KafkaAdminClient(
    bootstrap_servers=KAFKA_BROKER_URL,
    client_id='my_admin_client'
)

def delete_topic(admin_client, topic_name):
    try:
        admin_client.delete_topics([topic_name])
        print(f"Topic '{topic_name}' deleted successfully")
        return True
    except UnknownTopicOrPartitionError:
        print(f"Topic '{topic_name}' does not exist")
    except Exception as e:
        print(f"An error occurred while deleting the topic: {e}")

def create_topic(admin_client, topic_name, num_partitions, replication_factor):
    topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    )
    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists")
    except Exception as e:
        print(f"An error occurred while creating the topic: {e}")

try:
    # Delete the topic if it exists
    if delete_topic(admin_client, TOPIC_NAME):
        # Recreate the topic
        time.sleep(12)
        create_topic(admin_client, TOPIC_NAME, NUM_PARTITIONS, REPLICATION_FACTOR)
finally:
    # Close the admin client
    admin_client.close()
