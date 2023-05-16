using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaDtos;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaJsonEventsProducer;

public class Program
{
    private const string Topic = "users_json";
    private const string BootstrapServers = "localhost:9092,localhost:9093";

    public static async Task Main()
    {
        await CreateKafkaTopic();

        var cts = new CancellationTokenSource();

        var producer = Task.Run(() => StartProducer(cts.Token));

        Console.ReadKey();
        cts.Cancel();
        await producer;
    }

    private static async Task StartProducer(CancellationToken ct)
    {
        var producerConfig = new ProducerConfig { BootstrapServers = BootstrapServers };
        using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();

        var i = 1;
        while (!ct.IsCancellationRequested)
        {
            var userId = $"UserId_{i}";
            var message = new Message<Null, string>
            {
                Value = JsonSerializer.Serialize(new User
                {
                    UserId = userId,
                    Name = $"Name_{i}",
                })
            };
            await producer.ProduceAsync(Topic, message);

            i++;

            await Task.Delay(TimeSpan.FromSeconds(1));
        }
    }

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