# PURPOSE: Creates Kafka topics (mailboxes) for our platform
# File: kafka/topics.py

# producer.py  = reporter writes news article
# "crypto-events" topic = the mailbox
# consumer.py  = delivery boy picks from mailbox
# PostgreSQL   = the house receiving the news

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from api_service.config import get_settings
import logging
logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)
settings = get_settings()


# Steps-->
# 1 Connect to Kafka
#    → Create KafkaAdminClient using bootstrap server

# 2️ Get existing topics
#    → Ask Kafka: what topics already exist?

# 3️ Check topic presence
#    → If "crypto-events" NOT in existing_topics

# 4️ Create topic (if needed)
#    → Define topic (name, partitions, replication)
#    → Send create request to Kafka

# 5️ Handle safely
#    → If exists → skip
#    → Handle errors (race condition, Kafka failure)
#    → Log everything
def create_kafka_topics():
    admin_client = None                        # SUGGESTION 1: set to None first
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS
        )
        existing_topics = admin_client.list_topics()
        logger.info(f"Existing topics: {existing_topics}")

        if settings.KAFKA_TOPIC_CRYPTO not in existing_topics:
            topic = NewTopic(
                name=settings.KAFKA_TOPIC_CRYPTO,
                num_partitions=1,
                replication_factor=1
            )
            admin_client.create_topics([topic])
            logger.info(f"Topic created: {settings.KAFKA_TOPIC_CRYPTO}")

        else:
            logger.info(f"Topic already exists: {settings.KAFKA_TOPIC_CRYPTO}")

    except TopicAlreadyExistsError:
        logger.warning(f"Topic already exists (race condition): {settings.KAFKA_TOPIC_CRYPTO}")

    except Exception as e:
        logger.error(f"Failed to create topic: {e}")
        raise

    finally:                                   # SUGGESTION 2: always close cleanly
        if admin_client:
            admin_client.close()
            logger.info("Admin client closed")


if __name__ == "__main__":
    create_kafka_topics()




# File runs
#    ↓
# Required tools imported (KafkaAdminClient, logging, settings)
#    ↓
# Settings loaded from config.py (.env values)
#    ↓
# Logger initialized (for tracking execution)
#    ↓
# Main function create_kafka_topics() called
#    ↓
# Try block starts (for safe execution)
#    ↓
# Connect to Kafka broker (localhost:9092)
#    ↓
# Fetch existing topics from Kafka
#    ↓
# Check: Does "crypto-events" topic already exist?
#       ↓
#       YES → Log "Topic already exists" and skip creation
#       ↓
#       NO  → Create new topic with:
#               - 1 partition
#               - replication factor = 1
#    ↓
# Send topic creation request to Kafka
#    ↓
# Log success message in terminal
#    ↓
# Handle edge cases:
#       - TopicAlreadyExistsError (race condition)
#       - General exceptions (Kafka down, network error)
#    ↓
# If error occurs → log error and raise exception
#    ↓
# Execution completes safely
