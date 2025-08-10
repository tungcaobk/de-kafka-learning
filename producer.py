from __future__ import annotations

import configparser
import json
import signal
import sys
import time
from typing import List

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException, Message

from custom_log import setup_logger


logging = setup_logger("data_transfer", "producer.log")


# -------------------------
# Config helpers
# -------------------------

def create_producer(config: configparser.ConfigParser) -> Producer:
    main_kafka_config = dict(config["kafka_main"])
    producer_conf = dict(config["producer_config"])

    final_conf = {**main_kafka_config, **producer_conf}
    logging.info("Producer config: %s", json.dumps(final_conf, indent=4))
    return Producer(final_conf)


def create_consumer_to_remote(config: configparser.ConfigParser) -> Consumer:
    """Create a Consumer for the remote cluster by merging two sections."""
    source_kafka_conf = dict(config["kafka_remote"])
    consumer_conf = dict(config["remote_consumer_config"])

    final_conf = {**source_kafka_conf, **consumer_conf}
    logging.info("Source Consumer config: %s", json.dumps(final_conf, indent=4))
    return Consumer(final_conf)


# -------------------------
# Callbacks
# -------------------------

def _delivery_report(err: KafkaError | None, msg: Message) -> None:
    if err is not None:
        try:
            value_preview = msg.value().decode("utf-8")[:256]
        except Exception:
            value_preview = str(msg.value())[:256]
        logging.error("Delivery failed: %s | topic=%s partition=%s offset=%s value=%s",
                      err, msg.topic(), msg.partition(), msg.offset(), value_preview)
    else:
        logging.info("Delivered to %s[%s] @ offset %s",
                     msg.topic(), msg.partition(), msg.offset())


def _on_partition_assign(consumer: Consumer, partitions) -> None:
    for p in partitions:
        logging.info("ASSIGNED %s[%d] @ %s", p.topic, p.partition, p.offset)
    # If you need to seek to specific offsets, do it here then consumer.assign(partitions)
    # consumer.assign(partitions)

def _on_partition_revoke(consumer: Consumer, partitions) -> None:
    for p in partitions:
        logging.info("REVOKED %s[%d]", p.topic, p.partition)

# -------------------------
# Assignment wait helper (avoid blind sleep)
# -------------------------
def wait_until_assigned(consumer: Consumer, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline and not consumer.assignment():
        consumer.poll(0.25)  # serves rebalance callbacks
    if not consumer.assignment():
        raise TimeoutError("No partitions assigned within timeout")


# -------------------------
# Main app logic
# -------------------------

def main() -> int:
    # Read configuration
    config = configparser.ConfigParser(interpolation=None)
    read_files = config.read("config.ini")
    if not read_files:
        logging.error("config.ini not found in working directory")
        return 1

    source_topic_name = config["other"]["source.topic.name"]
    remote_topic = config["other"]["remote.topic.name"]

    try:
        batch_size = int(config["other"]["remote.max.poll.records"])
        remote_poll_timeout = int(config["other"]["remote.poll.timeout"])
    except Exception:
        logging.warning("max.poll.records missing/invalid, defaulting to %d", batch_size)
        raise Exception("max.poll.records missing/invalid")

    producer = create_producer(config)
    logging.info("Main Producer is running... Sending to '%s'", source_topic_name)

    consumer = create_consumer_to_remote(config)
    consumer.subscribe([remote_topic], on_assign=_on_partition_assign, on_revoke=_on_partition_revoke)
    logging.info("Remote Consumer is running... Listening on '%s'", remote_topic)

    # Wait for assignment (no arbitrary sleep)
    try:
        wait_until_assigned(consumer, timeout=20.0)
    except TimeoutError as e:
        logging.error("%s", e)
        return 1

    # Graceful shutdown setup
    stop = False

    def _signal_handler(sig, frame):
        nonlocal stop
        logging.warning("Received signal %s, shutting down...", sig)
        stop = True

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # Consume → Produce bridge
    try:
        while not stop:
        # for i in range(1, 3):
            messages: List[Message] = consumer.consume(num_messages=batch_size, timeout=remote_poll_timeout)
            if not messages:
                # serve delivery callbacks even when idle
                producer.poll(0)
                continue

            logging.info("Consumed batch: %d message(s)", len(messages))

            processed = 0
            POLL_INTERVAL = 100
            for msg in messages:
                if msg is None:
                    continue
                if msg.error():
                    err = msg.error()
                    if err.code() == KafkaError._PARTITION_EOF:
                        continue
                    if err.fatal():
                        logging.error("Fatal consumer error: %s", err)
                        raise KafkaException(err)
                    if err.retriable():
                        logging.warning("Retriable consumer error: %s", err)
                        continue
                    logging.error("Non-retriable consumer error: %s", err)
                    continue

                try:
                    producer.produce(
                        topic=source_topic_name,
                        # key=msg.key(),
                        value=msg.value(),
                        # headers=msg.headers(),
                        on_delivery=_delivery_report,
                    )
                    processed += 1
                    if (processed + 1) % POLL_INTERVAL == 0:
                        producer.poll(0)
                except BufferError:
                    # Local queue full: serve callbacks to drain and retry once
                    logging.warning("Producer queue is full; flushing callbacks and retrying")
                    producer.poll(0.5)
                    producer.produce(
                        topic=source_topic_name,
                        value=msg.value(),
                        on_delivery=_delivery_report,
                    )

            # Serve delivery callbacks and push IO
            producer.poll(0)

            # Commit consumer offsets for the whole batch *after* producing
            try:
                consumer.commit(asynchronous=True)
                logging.info("Committed offsets after processing %d msg(s)", processed)
            except KafkaException as e:
                logging.warning("Commit failed: %s", e)

    except Exception:
        logging.exception("Unhandled error in main loop")
        # Best-effort final commit of what we processed so far
        try:
            consumer.commit(asynchronous=False)
        except Exception as ce:
            logging.warning("Final sync commit failed: %s", ce)
        return 2
    finally:
        # Flush outstanding producer messages
        try:
            producer.flush(10)
        except Exception:
            logging.exception("Producer flush failed")
        try:
            consumer.close()
        except Exception:
            logging.exception("Consumer close failed")
        logging.info("Bridge stopped.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
