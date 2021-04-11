using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Data.SchemaRegistry;
using Confluent.Kafka;
using Microsoft.Azure.Data.SchemaRegistry.ApacheAvro;

namespace TimeOff.Core
{
    public class KafkaAvroAsyncSerializer<T> : IAsyncSerializer<T>
    {
        private readonly SchemaRegistryAvroObjectSerializer _serializer;

        public KafkaAvroAsyncSerializer(SchemaRegistryClient schemaRegistryClient, string schemaGroup,
            bool autoRegisterSchemas = true)
        {
            _serializer = new SchemaRegistryAvroObjectSerializer(
                schemaRegistryClient,
                schemaGroup,
                new SchemaRegistryAvroObjectSerializerOptions
                {
                    AutoRegisterSchemas = autoRegisterSchemas
                });
        }

        public async Task<byte[]> SerializeAsync(T data, SerializationContext context)
        {
            if (data == null)
            {
                return null;
            }

            // SchemaRegistryAvroObjectSerializer can only serialize GenericRecord or ISpecificRecord.
            if (data is string s)
            {
                return Encoding.ASCII.GetBytes(s);
            }

            await using var stream = new MemoryStream();
            await _serializer.SerializeAsync(stream, data, typeof(T), CancellationToken.None);
            return stream.ToArray();
        }
    }
}