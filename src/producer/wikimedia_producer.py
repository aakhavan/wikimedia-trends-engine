import json
import logging
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError
from sseclient import SSEClient
import requests # sseclient uses requests

# Configure basic logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WikimediaProducer:
    """
    A Kafka producer that fetches real-time events from Wikimedia's EventStreams
    and sends them to a specified Kafka topic.
    """
    WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

    def __init__(self, bootstrap_servers: str, topic_name: str):
        """
        Initializes the WikimediaProducer.

        Args:
            bootstrap_servers: The Kafka bootstrap servers (e.g., 'localhost:9092').
            topic_name: The Kafka topic to which messages will be sent.
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.producer = self._create_kafka_producer()

    def _create_kafka_producer(self) -> KafkaProducer | None:
        """
        Creates and returns a KafkaProducer instance.

        Returns:
            KafkaProducer instance if successful, None otherwise.
        """
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Ensure messages are acknowledged by all in-sync replicas
                retries=3,   # Retry sending a message up to 3 times
                retry_backoff_ms=1000 # Wait 1s before retrying
            )
            logger.info(f"KafkaProducer connected to {self.bootstrap_servers}")
            return producer
        except KafkaError as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred while creating Kafka producer: {e}")
            return None

    def send_message(self, message_data: dict):
        """
        Sends a message to the Kafka topic.

        Args:
            message_data: The data (as a dictionary) to send.
        """
        if not self.producer:
            logger.error("Producer is not initialized. Cannot send message.")
            return

        try:
            future = self.producer.send(self.topic_name, value=message_data)
            # Optional: Block for synchronous send & get metadata or handle async
            # record_metadata = future.get(timeout=10)
            # logger.debug(f"Message sent to topic '{record_metadata.topic}' partition {record_metadata.partition} offset {record_metadata.offset}")
        except KafkaError as e:
            logger.error(f"Error sending message to Kafka: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while sending message: {e}")


    def fetch_and_send_events(self):
        """
        Connects to the Wikimedia EventStream, fetches events, and sends them to Kafka.
        This method will run indefinitely until interrupted.
        """
        if not self.producer:
            logger.error("Producer is not initialized. Aborting event fetching.")
            return

        logger.info(f"Connecting to Wikimedia stream: {self.WIKIMEDIA_STREAM_URL}")
        try:
            # Using requests directly with sseclient for better control and error handling
            response = requests.get(self.WIKIMEDIA_STREAM_URL, stream=True, timeout=30)
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            client = SSEClient(response)

            for event in client.events():
                if event.event == 'message' and event.data:
                    try:
                        event_data = json.loads(event.data)
                        # You might want to filter or transform event_data here
                        # For example, only take events from specific wikis or types
                        # if event_data.get("wiki") == "enwiki":
                        logger.info(f"Received event: {event_data.get('meta', {}).get('id', 'N/A')}")
                        self.send_message(event_data)
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to decode JSON data: {event.data}")
                    except Exception as e:
                        logger.error(f"Error processing event: {e}")
                elif event.event == 'error':
                    logger.error(f"Stream error event: {event.data}")
                # Add a small delay to prevent overwhelming the producer or network if needed
                # time.sleep(0.01)

        except requests.exceptions.RequestException as e:
            logger.error(f"Connection error to Wikimedia stream: {e}")
            # Implement retry logic or graceful shutdown here
            time.sleep(10) # Wait before retrying or exiting
            # Potentially call fetch_and_send_events() again for a retry,
            # but be careful about infinite loops without proper backoff.
        except Exception as e:
            logger.error(f"An unexpected error occurred during event fetching: {e}")
        finally:
            self.close()

    def close(self):
        """
        Closes the Kafka producer.
        """
        if self.producer:
            logger.info("Closing Kafka producer...")
            self.producer.flush(timeout=10) # Wait for all messages to be sent
            self.producer.close(timeout=10)
            logger.info("Kafka producer closed.")

if __name__ == "__main__":
    # Configuration - ideally, move this to a config file or environment variables
    KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
    KAFKA_TOPIC = 'wikimedia_events' # Make sure this topic exists or auto-creation is enabled

    producer_app = WikimediaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic_name=KAFKA_TOPIC
    )

    try:
        producer_app.fetch_and_send_events()
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user. Shutting down...")
    except Exception as e:
        logger.error(f"Unhandled exception in main execution: {e}")
    finally:
        producer_app.close()