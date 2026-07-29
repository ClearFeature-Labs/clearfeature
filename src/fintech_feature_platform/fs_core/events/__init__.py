"""Self-contained event contract and publisher seam for the Kafka-first MVP.

This package defines the event models (``EntityRef``, ``ReportDescriptor``,
``FeatureComputeRequested``), the topic constants, and the ``EventPublisher`` seam
(``InMemoryEventPublisher`` for default tests; ``KafkaEventPublisher`` with a lazy
``confluent-kafka`` import). It contains no consumer/worker logic.
"""
