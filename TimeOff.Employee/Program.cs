using System;
using System.Globalization;
using System.Threading.Tasks;
using Azure.Data.SchemaRegistry;
using Azure.Identity;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Configuration;
using TimeOff.Core;
using TimeOff.Models;

namespace TimeOff.Employee
{
    internal class Program
    {
        private static AdminClientConfig _adminConfig;
        private static SchemaRegistryConfig _schemaRegistryConfig;
        private static ProducerConfig _producerConfig;
        private static ConfigReader _configReader;

        public static IConfiguration Configuration { get; private set; }

        private static async Task Main()
        {
            Console.WriteLine("TimeOff Employee Terminal\n");

            Configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", true, true)
                .Build();

            _configReader = new ConfigReader(Configuration);

            // Read configs
            _schemaRegistryConfig = _configReader.GetSchemaRegistryConfig();
            _producerConfig = _configReader.GetProducerConfig();

            // Azure EH does not support Kafka Admin APIs.
            if (_configReader.IsLocalEnvironment)
            {
                _adminConfig = _configReader.GetAdminConfig();
                await KafkaHelper.CreateTopicAsync(_adminConfig, ApplicationConstants.LeaveApplicationsTopicName, 3);
            }

            await AddMessagesAsync();
        }


        private static async Task AddMessagesAsync()
        {
            CachedSchemaRegistryClient cachedSchemaRegistryClient = null;
            KafkaAvroAsyncSerializer<string> kafkaAvroAsyncKeySerializer = null;
            KafkaAvroAsyncSerializer<LeaveApplicationReceived> kafkaAvroAsyncValueSerializer = null;

            if (_configReader.IsLocalEnvironment)
            {
                cachedSchemaRegistryClient = new CachedSchemaRegistryClient(_schemaRegistryConfig);
            }
            else
            {
                var schemaRegistryClientAz =
                    new SchemaRegistryClient(Configuration["SchemaRegistryUrlAz"], new DefaultAzureCredential());
                var schemaGroupName = Configuration["SchemaRegistryGroupNameAz"];
                kafkaAvroAsyncKeySerializer =
                    new KafkaAvroAsyncSerializer<string>(schemaRegistryClientAz, schemaGroupName);
                kafkaAvroAsyncValueSerializer =
                    new KafkaAvroAsyncSerializer<LeaveApplicationReceived>(schemaRegistryClientAz, schemaGroupName);
            }

            using var producer = new ProducerBuilder<string, LeaveApplicationReceived>(_producerConfig)
                .SetKeySerializer(_configReader.IsLocalEnvironment
                    ? new AvroSerializer<string>(cachedSchemaRegistryClient)
                    : kafkaAvroAsyncKeySerializer)
                .SetValueSerializer(_configReader.IsLocalEnvironment
                    ? new AvroSerializer<LeaveApplicationReceived>(cachedSchemaRegistryClient)
                    : kafkaAvroAsyncValueSerializer)
                .Build();
            while (true)
            {
                var empEmail = ReadLine.Read("Enter your employee Email (e.g. none@example-company.com): ",
                    "none@example.com").ToLowerInvariant();
                var empDepartment = ReadLine.Read("Enter your department code (HR, IT, OPS): ").ToUpperInvariant();
                var leaveDurationInHours =
                    int.Parse(ReadLine.Read("Enter number of hours of leave requested (e.g. 8): ", "8"));
                var leaveStartDate = DateTime.ParseExact(ReadLine.Read("Enter vacation start date (dd-mm-yy): ",
                    $"{DateTime.Today:dd-mm-yy}"), "dd-mm-yy", CultureInfo.InvariantCulture);

                var leaveApplication = new LeaveApplicationReceived
                {
                    EmpDepartment = empDepartment,
                    EmpEmail = empEmail,
                    LeaveDurationInHours = leaveDurationInHours,
                    LeaveStartDateTicks = leaveStartDate.Ticks
                };
                var partition = new TopicPartition(
                    ApplicationConstants.LeaveApplicationsTopicName,
                    new Partition((int) Enum.Parse<Departments>(empDepartment)));
                var result = await producer.ProduceAsync(partition,
                    new Message<string, LeaveApplicationReceived>
                    {
                        Key = $"{empEmail}-{DateTime.UtcNow.Ticks}",
                        Value = leaveApplication
                    });
                Console.WriteLine(
                    $"\nMsg: Your leave request is queued at offset {result.Offset.Value} in the Topic {result.Topic}:{result.Partition.Value}\n\n");
            }

            // ReSharper disable once FunctionNeverReturns
        }
    }
}