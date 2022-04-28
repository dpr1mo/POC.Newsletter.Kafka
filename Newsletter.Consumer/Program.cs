using Confluent.Kafka;
using Serilog;
using System;

namespace Newsletter.Consumer
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                GroupId = "Newsletter",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            Log.Information($"Consumer settings - GroupID:'{config.GroupId}' and  Broker:'{config.BootstrapServers}'");

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe("new-registration-newsletter");

            try
            {
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume();
                        Log.Information($"Consumed message '{consumeResult.Message.Value}' from the Topic:'{consumeResult.Topic}' in the partition:'{consumeResult.Partition}' and offset:'{consumeResult.Offset}' ");
                    }
                    catch (ConsumeException e)
                    {
                        Log.Error($"Error occured: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Log.Information("Clean consumer and commit done on offsets");
                consumer.Close();
            }
        }
    }
}