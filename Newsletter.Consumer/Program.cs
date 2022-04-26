using Confluent.Kafka;
using Serilog;
using System;

namespace Newsletter.Consumer
{
    class Program
    {
        public static void Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                GroupId = "Newsletter",
                BootstrapServers = "localhost:9092"
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe("New_Request");

            while (!false)
            {
                var consumeResult = consumer.Consume();
                Log.Information($"Consumed message '{consumeResult.Message.Value}' from the topic:'{consumeResult.Topic}' in the partition:'{consumeResult.Partition}' and offset:'{consumeResult.Offset}' ");
            }

            consumer.Close();

            try
            {
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume();
                        Log.Information($"Mensagem consumida é '{consumeResult.Message.Value}' do Topico:'{consumeResult.Topic}' na partition:'{consumeResult.Partition}' e offset:'{consumeResult.Offset}' ");
                    }
                    catch (ConsumeException e)
                    {
                        Log.Error($"Error occured: {e.Error.Reason}");
                    }
                }

            }

            catch (OperationCanceledException)
            {
                consumer.Close();
            }
        }
    }
}
