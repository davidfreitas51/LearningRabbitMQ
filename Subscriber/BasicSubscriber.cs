using System.Text.Json;
using System.Text;
using Models;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Subscriber;

public class BasicSubscribe()
{
    public static async Task Subscribe()
    {
        const string exchangeName = "pedido.exchange";
        const string queueName = "pedido.criados";
        const string routingKey = "pedido.criado";

        const string dlxExchangeName = "pedido.dlx.exchange";
        const string dlqQueueName = "pedido.dlq";
        const string dlxRoutingKey = "pedido.falha";

        var factory = new ConnectionFactory
        {
            HostName = "localhost",
            Port = 5672,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            AutomaticRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(10)
        };

        await using var connection = await factory.CreateConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();

        // Exchange principal
        await channel.ExchangeDeclareAsync(
            exchange: exchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null
        );

        Console.WriteLine("Exchange principal criada");

        // Dead Letter Exchange
        await channel.ExchangeDeclareAsync(
            exchange: dlxExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null
        );

        Console.WriteLine("DLX criada");

        // Dead Letter Queue
        await channel.QueueDeclareAsync(
            queue: dlqQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null
        );

        Console.WriteLine("DLQ criada");

        // Binding entre DLX e DLQ
        await channel.QueueBindAsync(
            queue: dlqQueueName,
            exchange: dlxExchangeName,
            routingKey: dlxRoutingKey,
            arguments: null
        );

        Console.WriteLine("DLQ conectada à DLX");

        // Configuração de Dead Letter da fila principal
        var mainQueueArgs = new Dictionary<string, object?>
        {
            { "x-dead-letter-exchange", dlxExchangeName },
            { "x-dead-letter-routing-key", dlxRoutingKey }
        };

        // Fila principal
        await channel.QueueDeclareAsync(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: mainQueueArgs
        );

        Console.WriteLine("Fila principal criada com configuração de DLX");

        // Binding entre Exchange principal e fila principal
        await channel.QueueBindAsync(
            queue: queueName,
            exchange: exchangeName,
            routingKey: routingKey,
            arguments: null
        );

        Console.WriteLine("Fila principal conectada à exchange");

        // Controle de mensagens não confirmadas
        await channel.BasicQosAsync(
            prefetchCount: 1,
            prefetchSize: 0,
            global: false
        );

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (model, ea) =>
        {
            try
            {
                var body = ea.Body.ToArray();
                var json = Encoding.UTF8.GetString(body);
                var pedido = JsonSerializer.Deserialize<Pedido>(json);

                if (pedido is null)
                {
                    throw new JsonException("Pedido desserializado como null");
                }

                Console.WriteLine("==========================================");
                Console.WriteLine($"[Consumer] Pedido recebido: {pedido.Id}");
                Console.WriteLine($"Cliente: {pedido.ClienteEmail}");
                Console.WriteLine($"Valor:   {pedido.ValorTotal:C}");
                Console.WriteLine($"Criado:  {pedido.DataCriacao:O}");
                Console.WriteLine("==========================================");

                await Task.Delay(2000);
                
                if (pedido.ValorTotal == 0)
                {
                    Console.WriteLine("[Consumer] Pedido rejeitado: valor total igual a zero.");
                
                    await channel.BasicNackAsync(
                        deliveryTag: ea.DeliveryTag,
                        multiple: false,
                        requeue: false // Não pode ter DLX e requeue ao mesmo tempo na mesma requisição. Ou volta pra fila original ou pra dead
                        // O normal é tentar retry com limite de tentativas
                    );
                
                    return;
                }
                
                await channel.BasicAckAsync(
                    deliveryTag: ea.DeliveryTag,
                    multiple: false
                );

                Console.WriteLine($"Pedido {pedido.Id} processado com sucesso");
            }
            catch (JsonException ex)
            {
                Console.WriteLine($"[Consumer] Erro de desserialização: {ex.Message}");

                await channel.BasicNackAsync(
                    deliveryTag: ea.DeliveryTag,
                    multiple: false,
                    requeue: false
                );
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Consumer] Erro ao processar pedido: {ex.Message}");

                await channel.BasicNackAsync(
                    deliveryTag: ea.DeliveryTag,
                    multiple: false,
                    requeue: false
                );
            }
        };

        await channel.BasicConsumeAsync(
            queue: queueName,
            autoAck: false,
            consumer: consumer
        );

        Console.WriteLine("Consumer iniciado, pressione ENTER para sair");

        Console.ReadLine();
    }
}