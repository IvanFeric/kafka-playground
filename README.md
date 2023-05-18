# kafka-playground
Kafka playground

The solution contains 5 projects:
KafkaDtos - just the User class that's being sent to Kafka as a message
KafkaCloudEventsProducer - producer console application that is using CloudEvents formatter for messages
KafkaCloudEventsConsumer - consumer console application that is using CloudEvents formatter for deserializing messages
KafkaJsonEventsProducer - producer console application that is using JSON serializer for messages
KafkaJsonEventsConsumer - consumer console application that is using JSON serializer for deserializing messages

There is also a docker-compose file for initial Kafka Docker setup.

The tests are perfomed to first run the producer console app (with `dotnet run`). 

After a while, start the corresponding consumer app (if you started CloudEvents producer, run CloudEvents consumer, otherwise run JSON consumer.

The messages are committed 10 at a time but we're processing them in real-time (by outputting their details to console).

To test the cooperative rebalancing scenario, you would spin up an additional consumer console app at the moment when the first consumer has uncommitted messages of both partitions. You should see the message when rebalancing happens and notice that the second consumer continued off at the first offset after the last one outputted by the first consumer.

If you kill one of the consumer processes, you can observe that approximatelly 45 seconds later the other consumer picked up the partition of the killed consumer.
