using System;
using System.Globalization;
using System.Net;
using System.Threading.Tasks;
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
        public static IConfiguration Configuration { get; private set; }

        private static async Task Main(string[] args)
        {
            Console.WriteLine("TimeOff Employee Terminal\n");

            Configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", true, true)
                .Build();

            // Read configs
            _adminConfig = Configuration.GetSection(nameof(AdminClientConfig)).Get<AdminClientConfig>();
            _schemaRegistryConfig = Configuration.GetSection(nameof(SchemaRegistryConfig)).Get<SchemaRegistryConfig>();
            _producerConfig = Configuration.GetSection(nameof(ProducerConfig)).Get<ProducerConfig>();
            _producerConfig.ClientId = Dns.GetHostName();

            await KafkaHelper.CreateTopicAsync(_adminConfig, ApplicationConstants.LeaveApplicationsTopicName, 3);
            await AddMessagesAsync();
        }

        private static async Task AddMessagesAsync()
        {
            using var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
            using var producer = new ProducerBuilder<string, LeaveApplicationReceived>(_producerConfig)
                .SetKeySerializer(new AvroSerializer<string>(schemaRegistry))
                .SetValueSerializer(new AvroSerializer<LeaveApplicationReceived>(schemaRegistry))
                .Build();
            while (true)
            {
                var empEmail = ReadLine.Read("Enter your employee Email (e.g. none@example-company.com): ",
                    "none@example.com").ToLowerInvariant();
                var empDepartment = ReadLine.Read("Enter your department code (HR, IT, OPS): ").ToUpperInvariant();
                var leaveDurationInHours =
                    int.Parse(ReadLine.Read("Enter number of hours of leave requested (e.g. 8): ", "8"));
                var leaveStartDate = DateTime.ParseExact(ReadLine.Read("Enter vacation start date (dd-mm-yy): ",
                    $"{DateTime.Today:dd-MM-yy}"), "dd-mm-yy", CultureInfo.InvariantCulture);

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