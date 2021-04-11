using System;
using System.Net;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Microsoft.Extensions.Configuration;

namespace TimeOff.Core
{
    public class ConfigReader
    {
        private readonly IConfiguration _configuration;

        public ConfigReader(IConfiguration configuration)
        {
            _configuration = configuration;
            IsLocalEnvironment = Convert.ToBoolean(configuration[nameof(IsLocalEnvironment)]);
        }

        public bool IsLocalEnvironment { get; }

        public ProducerConfig GetProducerConfig()
        {
            var config = IsLocalEnvironment
                ? _configuration.GetSection(nameof(ProducerConfig)).Get<ProducerConfig>()
                : _configuration.GetSection("ProducerConfigAz").Get<ProducerConfig>();

            config.ClientId = Dns.GetHostName();
            if (IsLocalEnvironment)
            {
                return config;
            }

            config.SecurityProtocol = SecurityProtocol.SaslSsl;
            config.SaslMechanism = SaslMechanism.Plain;
            return config;
        }

        public AdminClientConfig GetAdminConfig()
        {
            return _configuration.GetSection(nameof(AdminClientConfig)).Get<AdminClientConfig>();
        }

        public SchemaRegistryConfig GetSchemaRegistryConfig()
        {
            return _configuration.GetSection(nameof(SchemaRegistryConfig)).Get<SchemaRegistryConfig>();
        }

        public ConsumerConfig GetConsumerConfig()
        {
            var config = IsLocalEnvironment
                ? _configuration.GetSection(nameof(ConsumerConfig)).Get<ConsumerConfig>()
                : _configuration.GetSection("ConsumerConfigAz").Get<ConsumerConfig>();

            // Read messages from start if no commit exists.
            config.AutoOffsetReset = AutoOffsetReset.Earliest;

            if (IsLocalEnvironment)
            {
                return config;
            }

            config.SecurityProtocol = SecurityProtocol.SaslSsl;
            config.SaslMechanism = SaslMechanism.Plain;
            return config;
        }
    }
}