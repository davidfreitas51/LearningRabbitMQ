using Models;
using RabbitMQ.Client;

namespace Publisher;

public class BasicPublish
{
    public static async Task Publish()
    {
        const string exchangeName = "pedido.exchange";
        const string queueName = "pedido.criados";
        const string routingKey = "pedido.criado";

        const string dlxExchangeName = "pedido.dlx.exchange";
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

        await channel.ExchangeDeclareAsync(
            exchange: exchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null
        );

        Console.WriteLine("Exchange principal criada");

        var mainQueueArgs = new Dictionary<string, object?>
        {
            // Exchange que receberá mensagens rejeitadas ou expiradas
            { "x-dead-letter-exchange", dlxExchangeName },

            // Routing key utilizada no redirecionamento
            { "x-dead-letter-routing-key", dlxRoutingKey }
        };

        await channel.QueueDeclareAsync(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: mainQueueArgs
        );

        Console.WriteLine("Fila principal criada com configuração de DLX");

        await channel.QueueBindAsync(
            queue: queueName,
            exchange: exchangeName,
            routingKey: routingKey,
            arguments: null
        );

        Console.WriteLine("Fila conectada à exchange");

        Console.WriteLine("Quantos pedidos você quer enviar?");

        if (!int.TryParse(Console.ReadLine(), out var quantidadePedidos))
        {
            quantidadePedidos = 3;
        }

        for (int i = 1; i <= quantidadePedidos; i++)
        {
            var pedido = CriarPedidoFake(i);

            var body = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(pedido);

            var properties = new BasicProperties
            {
                Persistent = true,
                ContentType = "application/json",
                ContentEncoding = "utf-8"
            };

            await channel.BasicPublishAsync(
                exchange: exchangeName,
                routingKey: routingKey,
                mandatory: false,
                basicProperties: properties,
                body: body
            );

            Console.WriteLine($"Pedido {pedido.Id} enviado");
            Console.WriteLine("Pressione ENTER para enviar um novo pedido");

            Console.ReadLine();
        }

        static Pedido CriarPedidoFake(int index)
        {
            return new Pedido
            {
                Id = Guid.NewGuid(),
                ClienteEmail = $"cliente{index}@email.com",
                ValorTotal = Random.Shared.Next(0),
                DataCriacao = DateTime.UtcNow,
                Itens = new List<Item>
                {
                    new Item
                    {
                        NomeDoProduto = $"Produto {index}",
                        Quantidade = Random.Shared.Next(1, 5),
                        PrecoUnitario = Random.Shared.Next(20, 1000)
                    }
                }
            };
        }
    }
}
