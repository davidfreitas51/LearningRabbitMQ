using RabbitMQ.Client;

namespace Publisher;

public class PersistentQueue(){
    public static async Task Publish(){
        var factory = new ConnectionFactory
        {
            HostName = "localhost",
            Port = 5672,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            AutomaticRecoveryEnabled = true, // Se ele cair ou reiniciar, o client vai tentar se reconectar automaticamente
            NetworkRecoveryInterval = TimeSpan.FromSeconds(10) // De quanto tem quanto tempo o client vai tentar se reconectar
        };

        await using var connection = await factory.CreateConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync(
            queue: "persistent_queue",
            durable: true, // aqui a real mudança. A fila é persistente em disco, se cair/reiniciar  a fila volta, mas as mensagens também precisam ser persistentes
            exclusive: false, // múltiplas conexões podem ou não usar a mesma fila
            autoDelete: false, // a fila não será automaticamente quando não houver consumidores
            arguments: null
        );

        var message = "Mensagem de exemplo";
        var body = System.Text.Encoding.UTF8.GetBytes(message);
        var properties = new BasicProperties
        {
            Persistent = true // Ponto chave. A mensagem será gravada em disco
        };

        await channel.BasicPublishAsync(
            exchange: "",
            routingKey: "persistent_queue",
            mandatory: false,
            basicProperties: properties,
            body: body
        );

        Console.WriteLine("Mensagem enviada com sucesso");
    }
}
