import os
import time
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import NodeNotReadyError, KafkaError
from utils.utils_logger import logger  # Ensure your utils_logger file exists

#####################################
# Utility Functions
#####################################


def get_zookeeper_address() -> str:
    """Fetch Zookeeper address from environment variables."""
    return os.getenv("ZOOKEEPER_ADDRESS", "localhost:2181")


def get_kafka_broker_address() -> str:
    """Fetch Kafka broker address from environment variables."""
    return os.getenv("KAFKA_BROKER_ADDRESS", "localhost:9092")


def check_zookeeper_service_is_ready(zookeeper_address: str) -> bool:
    """
    Check if the Zookeeper service is ready.
    (Simple example; real-world checks may require additional logic.)
    """
    logger.info(f"Checking Zookeeper service at {zookeeper_address}...")
    # This could include a real-world connection check, e.g., using telnet or a specific library.
    time.sleep(1)  # Simulate waiting for service readiness
    return True


def check_kafka_service_is_ready(kafka_broker_address: str) -> bool:
    """Check if the Kafka broker service is ready."""
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker_address)
        admin_client.close()
        return True
    except KafkaError as e:
        logger.error(f"Error checking Kafka: {e}")
        return False


def verify_services():
    """
    Verify that Zookeeper and Kafka services are ready.
    """
    zookeeper_address = get_zookeeper_address()
    kafka_broker_address = get_kafka_broker_address()

    if not check_zookeeper_service_is_ready(zookeeper_address):
        logger.error(f"Zookeeper service at {zookeeper_address} is not ready. Exiting...")
        raise NodeNotReadyError(f"Zookeeper not ready at {zookeeper_address}.")

    if not check_kafka_service_is_ready(kafka_broker_address):
        logger.error(f"Kafka broker at {kafka_broker_address} is not ready. Exiting...")
        raise NodeNotReadyError(f"Kafka broker not ready at {kafka_broker_address}.")

    logger.info("Zookeeper and Kafka services are ready.")


def create_kafka_producer():
    """Create and return a Kafka producer instance."""
    kafka_broker_address = get_kafka_broker_address()
    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_broker_address,
            value_serializer=lambda v: str(v).encode("utf-8"),
        )
        logger.info(f"Kafka producer created with broker: {kafka_broker_address}")
        return producer
    except KafkaError as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return None


def create_kafka_topic(topic_name: str, num_partitions: int = 1, replication_factor: int = 1):
    """
    Create a Kafka topic if it doesn't already exist.
    """
    kafka_broker_address = get_kafka_broker_address()
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker_address)
        topics = admin_client.list_topics()

        if topic_name not in topics:
            new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            admin_client.create_topics([new_topic])
            logger.info(f"Kafka topic '{topic_name}' created.")
        else:
            logger.info(f"Kafka topic '{topic_name}' already exists.")
    except KafkaError as e:
        logger.error(f"Error creating Kafka topic '{topic_name}': {e}")
    finally:
        admin_client.close()
