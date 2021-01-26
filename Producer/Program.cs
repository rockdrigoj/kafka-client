using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Producer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:29092"
            };

            using var p = new ProducerBuilder<Null, string>(config).Build();

            for (var i = 0; i < 3; i++)
            {
                var msg = new Message<Null, string>
                {
                    Value = $"Message #{i}"
                };

                var dr = await p.ProduceAsync("test", msg);
                Console.WriteLine($"Produced '{dr.Value}' to topic {dr.Topic}, partition {dr.Partition}, offset {dr.Offset}");
            }

            Console.WriteLine("Done");
        }
    }
}
