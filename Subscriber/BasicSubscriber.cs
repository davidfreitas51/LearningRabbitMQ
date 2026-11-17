using System.Text.Json;
using Models;
using RabbitMQ.Client;

namespace Subscriber;

public class BasicSubscribe(){
    public static async Task Subscribe(){
        {
        
                const string exchangeName = "pedido.exchange";
                const string queueName = "pedido.criados";
                const string routingKey = "pedido.criado";
        
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
        
                await channel.ExchangeDeclareAsync(
                    exchange: exchangeName,
                    type: ExchangeType.Direct,
                    durable: true,
                    autoDelete: false,
                    arguments: null
                );
        
                await channel.QueueDeclareAsync(
                    queue: queueName,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null
                );
        
                await channel.QueueBindAsync(
                    queue: queueName,
                    exchange: exchangeName,
                    routingKey: routingKey,
                    arguments: null
                );
        
                await channel.BasicQosAsync(
                    prefetchCount: 1,
                    prefetchSize: 0,
                    global: false
                );
        
                var consumer = new RabbitMQ.Client.Events.AsyncEventingBasicConsumer(channel);
        
                consumer.ReceivedAsync += async (model, ea) =>
                {
        
                    try
                    {
                        var body = ea.Body.ToArray();
                        var json = System.Text.Encoding.UTF8.GetString(body);
                        var pedido = JsonSerializer.Deserialize<Pedido>(json);
        
                        Console.WriteLine("==========================================");
                        Console.WriteLine($"[Consumer] Pedido recebido: {pedido?.Id}");
                        Console.WriteLine($"Cliente: {pedido?.ClienteEmail}");
                        Console.WriteLine($"Valor:   {pedido?.ValorTotal:C}");
                        Console.WriteLine($"Criado:  {pedido?.DataCriacao:O}");
                        Console.WriteLine("==========================================");
        
                        await Task.Delay(2000);
        
                        await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
                    }
                    catch (JsonException ex)
                    {
                        Console.WriteLine($"[Consumer] Erro ao desserializar o pedido: {ex.Message}");
                        await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
                        throw;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[Consumer] Erro ao processar o pedido: {ex.Message}");
                        await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
                        throw;
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
}