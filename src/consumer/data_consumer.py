# src/consumer/data_consumer.py
import json
from kafka import KafkaConsumer
import logging
from . import config  # Import the config module

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# --- Configuration is now imported ---

def main():
    consumer = None
    try:
        consumer = KafkaConsumer(
            config.KAFKA_TOPIC,  # Use config.KAFKA_TOPIC
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,  # Use config.KAFKA_BOOTSTRAP_SERVERS
            auto_offset_reset='earliest',
            group_id=config.CONSUMER_GROUP_ID,  # Use config.CONSUMER_GROUP_ID
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            consumer_timeout_ms=1000
        )
        logger.info(f"Consumer started for topic '{config.KAFKA_TOPIC}' in group '{config.CONSUMER_GROUP_ID}'")
        logger.info("Waiting for messages...")

        for message in consumer:
            logger.info(f"Received: Partition={message.partition}, Offset={message.offset}, Key={message.key}")
            event_data = message.value
            if event_data and isinstance(event_data, dict) and 'title' in event_data:
                logger.info(f"  Event Title: {event_data['title']}")
            elif event_data and isinstance(event_data, dict) and 'meta' in event_data and 'uri' in event_data['meta']:
                logger.info(f"  Event URI: {event_data['meta']['uri']}")
            else:
                logger.info(f"  Event Data: {event_data}")

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user. Shutting down...")
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from message: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred in the consumer: {e}", exc_info=True)
    finally:
        if consumer:
            logger.info("Closing Kafka consumer.")
            consumer.close()


if __name__ == "__main__":
    main()
