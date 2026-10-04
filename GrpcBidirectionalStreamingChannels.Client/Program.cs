using Grpc.Core;
using Grpc.Net.Client;
using GrpcBidirectionalStreamingChannels;

var channel = GrpcChannel.ForAddress(
    "https://localhost:7001");

var client =
    new OrderStreaming.OrderStreamingClient(channel);

using var call = client.ProcessOrders();

Console.WriteLine(
    "Connected to gRPC server.");

var sendTask = SendOrdersAsync(
    call.RequestStream);

var receiveTask = ReceiveResponsesAsync(
    call.ResponseStream);

await Task.WhenAll(
    sendTask,
    receiveTask);

Console.WriteLine(
    "Streaming completed.");

Console.ReadLine();


static async Task SendOrdersAsync(
    IClientStreamWriter<OrderRequest> requestStream)
{
    for (int i = 1; i <= 10; i++)
    {
        var request = new OrderRequest
        {
            OrderId = $"ORD-{i:000}",
            Action = OrderAction.Create,
            ProductName = $"Product-{i}",
            Quantity = i
        };

        Console.WriteLine(
            $"[REQUEST] Sending {request.OrderId}");

        await requestStream.WriteAsync(request);

        await Task.Delay(100);
    }

    await requestStream.CompleteAsync();

    Console.WriteLine(
        "[REQUEST] Request stream completed.");
}


static async Task ReceiveResponsesAsync(
    IAsyncStreamReader<OrderResponse> responseStream)
{
    await foreach (
        var response in responseStream.ReadAllAsync())
    {
        Console.WriteLine(
            $"[RESPONSE] " +
            $"{response.OrderId} | " +
            $"{response.Status} | " +
            $"{response.WorkerId} | " +
            $"{response.Message}");
    }
}