using System;
using System.IO;
using System.Text;
using System.Threading;
using Azure.Data.SchemaRegistry;
using Confluent.Kafka;
using Microsoft.Azure.Data.SchemaRegistry.ApacheAvro;

namespace TimeOff.Core
{
    public class KafkaAvroDeserializer<T> : IDeserializer<T>
    {
        private readonly SchemaRegistryAvroObjectSerializer _serializer;

        public KafkaAvroDeserializer(SchemaRegistryClient schemaRegistryClient, string schemaGroup)
        {
            _serializer = new SchemaRegistryAvroObjectSerializer(schemaRegistryClient, schemaGroup);
        }

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (data.IsEmpty)
            {
                return default;
            }

            // SchemaRegistryAvroObjectSerializer can only serialize GenericRecord or ISpecificRecord.
            if (typeof(T) == typeof(string))
            {
                return (T) Convert.ChangeType(Encoding.ASCII.GetString(data.ToArray()), typeof(T));
            }

            return (T) _serializer.Deserialize(new MemoryStream(data.ToArray()), typeof(T), CancellationToken.None);
        }
    }
}