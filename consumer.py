from __future__ import annotations

import configparser
import json
import signal
import sys
import time
from typing import List

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from custom_log import setup_logger


logging = setup_logger("data_transfer", "consumer.log")


# -------------------------
# Kafka helpers
# -------------------------

def create_consumer(config: configparser.ConfigParser) -> Consumer:
    """Create a Consumer by merging base Kafka and consumer configs."""
    main_kafka_config = dict(config["kafka_main"])
    consumer_conf = dict(config["consumer_config"])
    final_conf = {**main_kafka_config, **consumer_conf}
    logging.info("Consumer config: %s", json.dumps(final_conf, indent=4))
    return Consumer(final_conf)


def on_partition_assign(consumer: Consumer, partitions) -> None:
    for p in partitions:
        logging.info("ASSIGNED %s[%d] @ %s", p.topic, p.partition, p.offset)


def on_partition_revoke(consumer: Consumer, partitions) -> None:
    for p in partitions:
        logging.info("REVOKED %s[%d]", p.topic, p.partition)


def wait_until_assigned(consumer: Consumer, timeout: float = 20.0) -> None:
    """Poll shortly until partitions are assigned or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline and not consumer.assignment():
        consumer.poll(0.25)  # serves rebalance callbacks
    if not consumer.assignment():
        raise TimeoutError("No partitions assigned within timeout")


# -------------------------
# Mongo helpers
# -------------------------

def connect_to_mongodb(config: configparser.ConfigParser):
    """Connect to MongoDB and return the collection object."""
    logging.info("Connecting MongoDB ...")
    mongo_config = config["mongodb"]
    client = MongoClient(mongo_config["connection.string"])  # add options as needed (tls, timeouts)

    # Ping
    client.admin.command("ping")
    logging.info("Ping MongoDB connection successful.")

    db = client[mongo_config["database.name"]]
    collection = db[mongo_config["collection.name"]]
    logging.info(
        "Connected to MongoDB: db='%s', collection='%s'",
        mongo_config["database.name"],
        mongo_config["collection.name"],
    )
    return collection


# -------------------------
# Main
# -------------------------

def main() -> int:
    # Read configuration
    config = configparser.ConfigParser(interpolation=None)
    read_files = config.read("config.ini")
    if not read_files:
        logging.error("config.ini not found in working directory")
        return 1

    source_topic_name = config["other"]["source.topic.name"]
    dlq_source_topic_name = config["other"]["source.dlq.topic.name"]

    try:
        max_poll_records = int(config["other"]["source.max.poll.records"])
        poll_timeout = int(config["other"]["source.poll.timeout"])
    except Exception:
        logging.warning("source.max.poll.records missing/invalid, defaulting to %d", max_poll_records)
        raise Exception("source.max.poll.records missing/invalid")

    # Connect to Mongo
    mongo_collection = connect_to_mongodb(config)

    # Kafka consumer
    consumer = create_consumer(config)
    consumer.subscribe([source_topic_name], on_assign=on_partition_assign, on_revoke=on_partition_revoke)

    try:
        wait_until_assigned(consumer, timeout=20.0)
    except TimeoutError as e:
        logging.error("%s", e)
        return 1

    # Graceful shutdown
    stop = False

    def _signal_handler(sig, frame):
        nonlocal stop
        logging.warning("Received signal %s, shutting down...", sig)
        stop = True

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # Consume loop
    try:
        while not stop:
            messages: List[Message] = consumer.consume(num_messages=max_poll_records, timeout=poll_timeout)
            if not messages:
                consumer.poll(0)  # serve callbacks
                continue

            logging.info("Consumed batch: %d message(s)", len(messages))

            documents = []
            for msg in messages:
                if msg is None:
                    continue

                if msg.error():
                    err = msg.error()
                    if err.code() == KafkaError._PARTITION_EOF:
                        continue
                    if err.fatal():
                        logging.error("Fatal consumer error: %s", err)
                        # TODO send to dlq
                        continue
                        # raise KafkaException(err)
                    if err.retriable():
                        logging.warning("Retriable consumer error: %s", err)
                        continue
                    logging.error("Non-retriable consumer error: %s", err)
                    continue

                # Parse JSON (skip invalid records but continue batch)
                try:
                    value_bytes = msg.value()
                    value_str = value_bytes.decode("utf-8") if isinstance(value_bytes, (bytes, bytearray)) else str(value_bytes)
                    data = json.loads(value_str)
                    documents.append(data)
                except json.JSONDecodeError:
                    logging.error("Invalid JSON, skipping. topic=%s partition=%s offset=%s", msg.topic(), msg.partition(), msg.offset())
                except Exception as e:
                    logging.exception("Unexpected parse error (skipping): %s", e)
                # except Exception as e:
                #     logging.error("Failed to parse/prepare message; sending to DLQ: %s", e)
                #     headers = (msg.headers() or []) + [
                #         ("dlq-error", str(e).encode()),
                #         ("dlq-origin-topic", msg.topic().encode()),
                #         ("dlq-origin-partition", str(msg.partition()).encode()),
                #         ("dlq-origin-offset", str(msg.offset()).encode()),
                #     ]
                #     # Send to DLQ using a separate producer if needed
                #     # producer.produce(dlq_topic, key=msg.key(), value=msg.value(), headers=headers)

            if not documents:
                continue

            # Insert batch into MongoDB
            try:
                result = mongo_collection.insert_many(documents, ordered=False)
                inserted = len(result.inserted_ids)
                logging.info("Inserted %d/%d documents into MongoDB", inserted, len(documents))

                # Commit offsets AFTER successful insert
                consumer.commit(asynchronous=True)
                logging.info("Committed batch offsets")

            except PyMongoError as e:
                # Do not commit; the batch will be reprocessed (at-least-once)
                logging.exception("Mongo insert failed; offsets not committed. Error: %s", e)
                # Optional: implement per-document retry or DLQ here
            except Exception as e:
                logging.exception("Unexpected error during insert; offsets not committed: %s", e)

    except KeyboardInterrupt:
        logging.warning("Interrupted by user")
    except Exception:
        logging.exception("Unhandled error in main loop")
        # Optionally do a final sync commit of the last safe point if you buffer spans
        return 2
    finally:
        try:
            consumer.close()
        except Exception:
            logging.exception("Consumer close failed")
        logging.info("Consumer stopped.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
