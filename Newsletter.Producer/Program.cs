using Confluent.Kafka;
using Serilog;
using System;
using System.Threading.Tasks;

namespace Newsletter.Producer
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    var newRequest = await producer.ProduceAsync("New_Request", new Message<Null, string> { Value = "Request Newsletter" });
                    Log.Information($"Delivery '{newRequest.Value}' for topic:'{newRequest.Topic}' partition:'{newRequest.Partition}' e offset:'{newRequest.Offset}' ");
                }
                catch (ProduceException<Null, string> e)
                {
                    Log.Information($"Delivery Fail: {e.Error.Reason}");
                }
            }
            Console.ReadLine();
        }
    }
}
