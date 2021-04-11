using System;
using System.Text.Json;
using Azure.Data.SchemaRegistry;
using Azure.Identity;
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
        private static void Main()
        {
            CachedSchemaRegistryClient cachedSchemaRegistryClient = null;
            KafkaAvroDeserializer<string> kafkaAvroKeyDeserializer = null;
            KafkaAvroDeserializer<LeaveApplicationProcessed> kafkaAvroValueDeserializer = null;

            Console.WriteLine("TimeOff Results Terminal\n");

            var configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", true, true)
                .Build();

            var configReader = new ConfigReader(configuration);

            var schemaRegistryConfig = configReader.GetSchemaRegistryConfig();
            var consumerConfig = configReader.GetConsumerConfig();

            if (configReader.IsLocalEnvironment)
            {
                cachedSchemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig);
            }
            else
            {
                var schemaRegistryClientAz =
                    new SchemaRegistryClient(configuration["SchemaRegistryUrlAz"], new DefaultAzureCredential());
                var schemaGroupName = configuration["SchemaRegistryGroupNameAz"];
                kafkaAvroKeyDeserializer =
                    new KafkaAvroDeserializer<string>(schemaRegistryClientAz, schemaGroupName);
                kafkaAvroValueDeserializer =
                    new KafkaAvroDeserializer<LeaveApplicationProcessed>(schemaRegistryClientAz, schemaGroupName);
            }

            using var consumer = new ConsumerBuilder<string, LeaveApplicationProcessed>(consumerConfig)
                .SetKeyDeserializer(configReader.IsLocalEnvironment
                    ? new AvroDeserializer<string>(cachedSchemaRegistryClient).AsSyncOverAsync()
                    : kafkaAvroKeyDeserializer)
                .SetValueDeserializer(configReader.IsLocalEnvironment
                    ? new AvroDeserializer<LeaveApplicationProcessed>(cachedSchemaRegistryClient).AsSyncOverAsync()
                    : kafkaAvroValueDeserializer)
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