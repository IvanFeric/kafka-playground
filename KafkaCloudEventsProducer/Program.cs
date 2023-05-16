using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Extensions;
using CloudNative.CloudEvents.Kafka;
using CloudNative.CloudEvents.SystemTextJson;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaDtos;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaCloudEventsProducer;

public class Program
{
    private const string Topic = "users";
    private const string BootstrapServers = "localhost:9092,localhost:9093";

    public static async Task Main()
    {
        await CreateKafkaTopic();

        var cts = new CancellationTokenSource();
        var formatter = new JsonEventFormatter<User>(SerializationOptions, new JsonDocumentOptions());

        var producer = Task.Run(() => StartProducer(formatter, cts.Token));

        Console.ReadKey();
        cts.Cancel();
        await producer;
    }

    private static async Task StartProducer(JsonEventFormatter formatter, CancellationToken ct)
    {
        var producerConfig = new ProducerConfig { BootstrapServers = BootstrapServers };
        using var producer = new ProducerBuilder<string, byte[]>(producerConfig).Build();

        var i = 1;
        while (!ct.IsCancellationRequested)
        {
            var userId = $"UserId_{i}";
            var cloudEvent = new CloudEvent
            {
                Id = Guid.NewGuid().ToString(),
                Type = "event-type",
                Source = new Uri("https://cloudevents.io/"),
                Time = DateTimeOffset.UtcNow,
                DataContentType = "application/cloudevents+json",
                Data = new User
                {
                    UserId = userId,
                    Name = $"Name_{i}",
                }
            };
            cloudEvent.SetPartitionKey(userId);
            var kafkaMessage = cloudEvent.ToKafkaMessage(ContentMode.Structured, formatter);
            await producer.ProduceAsync(Topic, kafkaMessage);
                
            i++;

            await Task.Delay(TimeSpan.FromSeconds(1));
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