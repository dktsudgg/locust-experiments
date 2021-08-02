import os
import random
import string
import time
from decouple import config

from locust import TaskSet, task, events, User

from additional_handlers import additional_success_handler, additional_failure_handler
from kafka_client import KafkaClient

WORK_DIR = os.path.dirname(__file__)

# read kafka brokers from config
# KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092").split(sep=",")
KAFKA_BROKERS = config('KAFKA_BROKERS', default="kafka:9092", cast=str).split(sep=",")

# read other environment variables
# QUIET_MODE = True if os.getenv("QUIET_MODE", "true").lower() in ['1', 'true', 'yes'] else False
# TASK_DELAY = int(os.getenv("TASK_DELAY", "0"))
QUIET_MODE = True if config("QUIET_MODE", default="true", cast=str) in ['1', 'true', 'yes'] else False
TASK_DELAY = config("TASK_DELAY", default=0, cast=int)


# register additional logging handlers
if not QUIET_MODE:
    events.request_success += additional_success_handler
    events.request_failure += additional_failure_handler


class KafkaLocust(User):
    client = None
    abstract = True

    def __init__(self, *args, **kwargs):
        super(KafkaLocust, self).__init__(*args, **kwargs)
        if not KafkaLocust.client:
            KafkaLocust.client = KafkaClient(KAFKA_BROKERS)


class KafkaBehaviour(TaskSet):

    def random_message(self, min_length=32, max_length=128):
        return ''.join(random.choice(string.ascii_uppercase) for _ in range(random.randrange(min_length, max_length)))

    def timestamped_message(self):
        return f"{time.time() * 1000}:" + ("kafka" * 24)[:random.randint(32, 128)]

    @task
    def task1(self):
        self.client.send("test-topic", message=self.timestamped_message())

    @task
    def task2(self):
        self.client.send("test-topic", message=self.timestamped_message(), key="key")


class KafkaUser(KafkaLocust):
    """
    Locust user class that pushes messages to Kafka
    """
    tasks = [KafkaBehaviour]
    min_wait = TASK_DELAY
    max_wait = TASK_DELAY
