using Confluent.Kafka;
using Serilog;
using System;
using System.Threading.Tasks;

namespace Newsletter.Producer
{
    internal class Program
    {
        public static async Task Main(string[] args)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    var newEmailRegistration = await producer.ProduceAsync("new-registration-newsletter", new Message<Null, string> { Value = "poc@kafka.com" });
                    Log.Information($"Delivery '{newEmailRegistration.Value}' for topic:'{newEmailRegistration.Topic}' partition:'{newEmailRegistration.Partition}' e offset:'{newEmailRegistration.Offset}' ");
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