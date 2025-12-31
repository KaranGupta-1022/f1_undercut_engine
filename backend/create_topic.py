from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

admin = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id="f1_admin",
    request_timeout_ms=30000,
    connections_max_idle_ms=30000,
    api_version_auto_timeout_ms=10000
)
topic = NewTopic(name="f1-telemetry", num_partitions=1, replication_factor=1)


try:
    admin.create_topics(new_topics=[topic], validate_only=False, timeout_ms=30000)
    print(f"Created topic: {topic.name}")
except TopicAlreadyExistsError:
    print(f"Topic already exists: {topic.name}")
finally:
    admin.close()