# src/consumer/data_consumer.py
import json
import logging
from kafka import KafkaConsumer
from . import config
# --- MODIFIED IMPORT ---
# Import from the new utils module and alias it for convenience
from src.utils import kafka_writer as writers

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """
    Main function to run the Kafka consumer, filter for edit events,
    and save them using a specified writer.
    """
    # --- CHOOSE YOUR WRITER HERE ---
    # The code below works without changes because of the import alias.
    writer = writers.JSONWriter('edit_events.jsonl')

    consumer = None
    processed_message_count = 0
    try:
        consumer = KafkaConsumer(
            config.KAFKA_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            group_id=config.CONSUMER_GROUP_ID,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            consumer_timeout_ms=2000
        )
        logger.info(f"Consumer started for topic '{config.KAFKA_TOPIC}'...")
        logger.info("Waiting for edit events to save...")

        for message in consumer:
            event_data = message.value

            if event_data.get('type') != 'edit':
                continue  # Skip non-edit events

            processed_message_count += 1
            logger.info(f"Processing Edit Event (Count: {processed_message_count}) for page: {event_data.get('title', 'N/A')}")

            # Delegate saving to our chosen writer
            writer.save(event_data)

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user. Shutting down...")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        if writer:
            writer.close()
        if consumer:
            logger.info(f"Closing Kafka consumer. Total edit events processed: {processed_message_count}")
            consumer.close()

if __name__ == "__main__":
    main()