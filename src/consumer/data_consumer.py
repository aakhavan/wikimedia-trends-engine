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
    processed_message_count = 0
    try:
        consumer = KafkaConsumer(
            config.KAFKA_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            group_id=config.CONSUMER_GROUP_ID,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            consumer_timeout_ms=1000
        )
        logger.info(f"Consumer started for topic '{config.KAFKA_TOPIC}' in group '{config.CONSUMER_GROUP_ID}'")
        logger.info("Waiting for messages...")

        for message in consumer:
            event_data = message.value

            # <--- NEW: Filter for 'edit' events only --->
            # Use .get() for safe access in case 'type' key is missing
            if event_data.get('type') != 'edit':
                # Optional: log that we are skipping a message
                # logger.info(f"Skipping event of type '{event_data.get('type')}'")
                continue # Skip to the next message

            processed_message_count += 1
            logger.info(f"Received Edit Event (Count: {processed_message_count}): Partition={message.partition}, Offset={message.offset}")

            # Since we know it's an edit, we can more reliably access keys
            # Use .get() as a good practice to avoid KeyErrors
            title = event_data.get('title', 'N/A')
            user = event_data.get('user', 'N/A')
            bot = event_data.get('bot', False)

            logger.info(f"  -> Page: {title} | Edited by: {user} | Is Bot: {bot}")


    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user. Shutting down...")
    except json.JSONDecodeError as e:
        # This can happen if a message is not valid JSON
        logger.error(f"Error decoding JSON from message: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred in the consumer: {e}", exc_info=True)
    finally:
        if consumer:
            logger.info(f"Closing Kafka consumer. Total edit events processed in this session: {processed_message_count}")
            consumer.close()


if __name__ == "__main__":
    main()