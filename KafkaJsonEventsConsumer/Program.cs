using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaJsonEventsConsumer;

public class Program
{
    private const string Topic = "users_json";
    private const string BootstrapServers = "localhost:9092,localhost:9093";

    public static async Task Main()
    {
        await CreateKafkaTopic();

        var cts = new CancellationTokenSource();

        var consumer = Task.Run(() => StartConsumer(cts.Token));

        Console.ReadKey();
        cts.Cancel();
        await consumer;
    }

    private static void StartConsumer(CancellationToken ct)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            GroupId = "cgid",
            EnableAutoCommit = true,
            EnableAutoOffsetStore = false
        };
        List<ConsumeResult<Null, string>> results = new();
        List<TopicPartition> lostPartitions = new();

        using var consumer = new ConsumerBuilder<Null, string>(consumerConfig)
            .SetPartitionsRevokedHandler((consumer, topicPartitionOffsets) =>
            {
                Console.WriteLine($"Partitions revoked! Consumer {consumer.Name}: {string.Join(", ", consumer.Assignment.Select(a => $"({a.Topic}, {a.Partition})"))}\r\nTopicPartitions: {string.Join(", ", topicPartitionOffsets.Select(a => $"({a.Topic}, {a.Partition})"))}");

                try
                {
                    foreach (var topicPartition in consumer.Assignment)
                    {
                        if (topicPartitionOffsets.Any(tpo => topicPartition == tpo.TopicPartition))
                        {
                            lostPartitions.Add(topicPartition);
                            Console.WriteLine($"Committing ({topicPartition.Topic}, {topicPartition.Partition})");
                            var result = results.LastOrDefault(r => r.Topic == topicPartition.Topic && r.Partition == topicPartition.Partition);
                            if (result != null)
                            {
                                consumer.StoreOffset(result);
                                Console.WriteLine("Committed");
                            }
                            else
                            {
                                Console.WriteLine("Nothing to commit");
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Exception when committing:\n{ex}");
                }
            })
            .Build();
        consumer.Subscribe(new[] { Topic });

        int batchNumber = 0;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(100));
                if (consumeResult is null) continue;

                if (batchNumber == 0)
                {
                    results = new() { consumeResult };
                    lostPartitions = new();
                }
                else
                {
                    results.Add(consumeResult);
                }

                batchNumber = (batchNumber + 1) % 10;

                var consumedMessage = consumeResult.Message;

                var partition = consumeResult.Partition;
                var key = consumeResult.Message.Key;
                var offset = consumeResult.Offset;

                Console.WriteLine($"Partition: {partition} Key: {key} Offset: {offset} Data: {consumedMessage.Value}");

                if (batchNumber == 0)
                {
                    StoreOffsets();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception when consuming:\n{ex}");
            }
        }

        StoreOffsets();
        consumer.Close();

        void StoreOffsets()
        {
            var filteredResults = results.Where(r => !lostPartitions.Any(tp => r.Topic == tp.Topic && r.Partition == tp.Partition)).ToList();

            var resultsWithLatestOffsetPerPartition = filteredResults.GroupBy(x => x.Partition.Value)
                .Select(group => group.MaxBy(y => y.Offset.Value))
                .Where(x => x != null);
            foreach (var result in resultsWithLatestOffsetPerPartition)
            {
                Console.WriteLine($"Storing offset for topic {result.Topic}, partition {result.Partition}");
                consumer.StoreOffset(result);
            }
        }
    }

    private static JsonSerializerOptions SerializationOptions => new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private static async Task CreateKafkaTopic()
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = BootstrapServers
        };

        var builder = new AdminClientBuilder(config);
        var client = builder.Build();
        try
        {
            await client.CreateTopicsAsync(new List<TopicSpecification>
            {
                new()
                {
                    Name = Topic,
                    ReplicationFactor = 1,
                    NumPartitions = 2
                }
            });
        }
        catch (CreateTopicsException e)
        {
            // do nothing in case of topic already exist
        }
        finally
        {
            client.Dispose();
        }
    }
}