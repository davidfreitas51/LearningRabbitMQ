using System.Text;
using System.Text.Json;
using Models;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Subscriber;

public class BasicSubscribe
{
    public static async Task Subscribe()
    {
        // Exchanges e filas
        const string exchangeName = "pedido.exchange";
        const string queueName = "pedido.criados";
        const string routingKey = "pedido.criado";

        const string dlxExchangeName = "pedido.dlx.exchange";
        const string dlqQueueName = "pedido.dlq";
        const string dlxRoutingKey = "pedido.falha";

        const string retryExchangeName = "pedido.retry.exchange";
        const string retryQueueName = "pedido.retry";
        const string retryRoutingKey = "pedido.retry";

        // Configurações de retry
        const int maxRetryAttempts = 3;
        const int retryDelayMs = 5000;
        const string retryHeader = "x-retry-count";

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

        // =====================================================
        // 1. EXCHANGE PRINCIPAL
        // =====================================================

        await channel.ExchangeDeclareAsync(
            exchange: exchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false
        );

        // =====================================================
        // 2. EXCHANGE DE RETRY
        // =====================================================

        await channel.ExchangeDeclareAsync(
            exchange: retryExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false
        );

        // =====================================================
        // 3. DEAD LETTER EXCHANGE
        // =====================================================

        await channel.ExchangeDeclareAsync(
            exchange: dlxExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false
        );

        // =====================================================
        // 4. DEAD LETTER QUEUE
        // =====================================================

        await channel.QueueDeclareAsync(
            queue: dlqQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false
        );

        await channel.QueueBindAsync(
            queue: dlqQueueName,
            exchange: dlxExchangeName,
            routingKey: dlxRoutingKey
        );

        // =====================================================
        // 5. RETRY QUEUE
        // =====================================================

        var retryQueueArgs = new Dictionary<string, object?>
        {
            // Tempo que a mensagem ficará aguardando na fila
            ["x-message-ttl"] = retryDelayMs,

            // Após expirar, volta para a exchange principal
            ["x-dead-letter-exchange"] = exchangeName,
            ["x-dead-letter-routing-key"] = routingKey
        };

        await channel.QueueDeclareAsync(
            queue: retryQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: retryQueueArgs
        );

        await channel.QueueBindAsync(
            queue: retryQueueName,
            exchange: retryExchangeName,
            routingKey: retryRoutingKey
        );

        // =====================================================
        // 6. FILA PRINCIPAL
        // =====================================================

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
            routingKey: routingKey
        );

        Console.WriteLine("Topologia RabbitMQ configurada.");

        // =====================================================
        // 7. CONFIGURAÇÃO DO CONSUMER
        // =====================================================

        await channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: 1,
            global: false
        );

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (_, ea) =>
        {
            var body = ea.Body.ToArray();

            var retryCount = GetRetryCount(
                ea.BasicProperties.Headers,
                retryHeader
            );

            try
            {
                var json = Encoding.UTF8.GetString(body);

                var pedido = JsonSerializer.Deserialize<Pedido>(json);

                if (pedido is null)
                {
                    throw new JsonException(
                        "Pedido desserializado como null."
                    );
                }

                Console.WriteLine("==========================================");
                Console.WriteLine($"[Consumer] Pedido recebido: {pedido.Id}");
                Console.WriteLine($"Cliente: {pedido.ClienteEmail}");
                Console.WriteLine($"Valor:   {pedido.ValorTotal:C}");
                Console.WriteLine($"Criado:  {pedido.DataCriacao:O}");
                Console.WriteLine($"Tentativa: {retryCount + 1}/{maxRetryAttempts}");
                Console.WriteLine("==========================================");

                await Task.Delay(2000);

                // Erro permanente: não adianta tentar novamente.
                if (pedido.ValorTotal <= 0)
                {
                    throw new InvalidDataException(
                        "Valor total do pedido deve ser maior que zero."
                    );
                }

                // Simula uma falha temporária de processamento.
                // Remova este trecho quando não precisar mais testar.
                // throw new Exception("Falha temporária simulada.");

                Console.WriteLine(
                    $"[Consumer] Pedido {pedido.Id} processado com sucesso."
                );

                await channel.BasicAckAsync(
                    deliveryTag: ea.DeliveryTag,
                    multiple: false
                );
            }
            catch (JsonException ex)
            {
                Console.WriteLine(
                    $"[Consumer] JSON inválido: {ex.Message}"
                );

                await SendToDlqAsync(
                    channel,
                    ea,
                    body,
                    dlxExchangeName,
                    dlxRoutingKey
                );
            }
            catch (InvalidDataException ex)
            {
                Console.WriteLine(
                    $"[Consumer] Mensagem inválida: {ex.Message}"
                );

                await SendToDlqAsync(
                    channel,
                    ea,
                    body,
                    dlxExchangeName,
                    dlxRoutingKey
                );
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    $"[Consumer] Falha no processamento: {ex.Message}"
                );

                if (retryCount < maxRetryAttempts - 1)
                {
                    var nextRetryCount = retryCount + 1;

                    Console.WriteLine(
                        $"[Consumer] Enviando para retry. Próxima tentativa: {nextRetryCount + 1}/{maxRetryAttempts}"
                    );

                    await RepublishAsync(
                        channel,
                        ea,
                        body,
                        retryExchangeName,
                        retryRoutingKey,
                        retryHeader,
                        nextRetryCount
                    );
                }
                else
                {
                    Console.WriteLine(
                        "[Consumer] Limite de tentativas atingido. Enviando para DLQ."
                    );

                    await SendToDlqAsync(
                        channel,
                        ea,
                        body,
                        dlxExchangeName,
                        dlxRoutingKey
                    );
                }
            }
        };

        await channel.BasicConsumeAsync(
            queue: queueName,
            autoAck: false,
            consumer: consumer
        );

        Console.WriteLine("Consumer iniciado. Pressione ENTER para sair.");

        Console.ReadLine();
    }

    // =========================================================
    // LÊ O CONTADOR DE RETRY
    // =========================================================

    private static int GetRetryCount(
        IDictionary<string, object?>? headers,
        string headerName)
    {
        if (headers is null ||
            !headers.TryGetValue(headerName, out var value) ||
            value is null)
        {
            return 0;
        }

        return Convert.ToInt32(value);
    }

    // =========================================================
    // REPUBLICA NA FILA DE RETRY
    // =========================================================

    private static async Task RepublishAsync(
        IChannel channel,
        BasicDeliverEventArgs ea,
        byte[] body,
        string exchange,
        string routingKey,
        string retryHeader,
        int retryCount)
    {
        var properties = CreateProperties(
            ea.BasicProperties,
            retryHeader,
            retryCount
        );

        await channel.BasicPublishAsync(
            exchange: exchange,
            routingKey: routingKey,
            mandatory: true,
            basicProperties: properties,
            body: body
        );

        // Só confirma a original depois de republicar.
        await channel.BasicAckAsync(
            deliveryTag: ea.DeliveryTag,
            multiple: false
        );
    }

    // =========================================================
    // ENVIA PARA A DLQ
    // =========================================================

    private static async Task SendToDlqAsync(
        IChannel channel,
        BasicDeliverEventArgs ea,
        byte[] body,
        string exchange,
        string routingKey)
    {
        var properties = CreateProperties(
            ea.BasicProperties
        );

        await channel.BasicPublishAsync(
            exchange: exchange,
            routingKey: routingKey,
            mandatory: true,
            basicProperties: properties,
            body: body
        );

        // Confirma a original somente após republicar.
        await channel.BasicAckAsync(
            deliveryTag: ea.DeliveryTag,
            multiple: false
        );
    }

    // =========================================================
    // COPIA AS PROPRIEDADES E HEADERS DA MENSAGEM
    // =========================================================

    private static BasicProperties CreateProperties(
        IReadOnlyBasicProperties original,
        string? retryHeader = null,
        int retryCount = 0)
    {
        var headers = new Dictionary<string, object?>();

        if (original.Headers is not null)
        {
            foreach (var header in original.Headers)
            {
                headers[header.Key] = header.Value;
            }
        }

        if (retryHeader is not null)
        {
            headers[retryHeader] = retryCount;
        }

        return new BasicProperties
        {
            Persistent = true,
            ContentType = original.ContentType,
            ContentEncoding = original.ContentEncoding,
            CorrelationId = original.CorrelationId,
            MessageId = original.MessageId,
            Type = original.Type,
            Headers = headers
        };
    }
}