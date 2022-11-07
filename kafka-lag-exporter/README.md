Prometheus Kafka Consumer Group Exporter


# Metrics
Ten main metrics are exported:

### `kafka_consumer_group_offset{group, topic, partition}` (gauge)
The latest committed offset of a consumer group in a given partition of a topic, as read from `__consumer_offsets`. Useful for calculating the consumption rate and lag of a consumer group.

### `kafka_consumer_group_lag{group, topic, partition}` (gauge)
The lag of a consumer group behind the head of a given partition of a topic - the difference between `kafka_topic_highwater` and `kafka_consumer_group_offset`. Useful for checking if a consumer group is keeping up with a topic.
