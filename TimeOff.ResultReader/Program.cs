using System;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Configuration;
using TimeOff.Core;
using TimeOff.Models;

namespace TimeOff.ResultReader
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Console.WriteLine("TimeOff Results Terminal\n");

            var configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", true, true)
                .Build();

            var schemaRegistryConfig =
                configuration.GetSection(nameof(SchemaRegistryConfig)).Get<SchemaRegistryConfig>();
            var consumerConfig = configuration.GetSection(nameof(ConsumerConfig)).Get<ConsumerConfig>();
            // Read messages from start if no commit exists.
            consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;

            using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
            using var consumer = new ConsumerBuilder<string, LeaveApplicationProcessed>(consumerConfig)
                .SetKeyDeserializer(new AvroDeserializer<string>(schemaRegistry).AsSyncOverAsync())
                .SetValueDeserializer(new AvroDeserializer<LeaveApplicationProcessed>(schemaRegistry).AsSyncOverAsync())
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                .Build();
            {
                try
                {
                    Console.WriteLine("");
                    consumer.Subscribe(ApplicationConstants.LeaveApplicationResultsTopicName);
                    while (true)
                    {
                        var result = consumer.Consume();
                        var leaveRequest = result.Message.Value;
                        Console.WriteLine(
                            $"Received message: {result.Message.Key} Value: {JsonSerializer.Serialize(leaveRequest)}");
                        consumer.Commit(result);
                        consumer.StoreOffset(result);
                        Console.WriteLine("\nOffset committed");
                        Console.WriteLine("----------\n\n");
                    }
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Consume error: {e.Error.Reason}");
                }
                finally
                {
                    consumer.Close();
                }
            }
        }
    }
}