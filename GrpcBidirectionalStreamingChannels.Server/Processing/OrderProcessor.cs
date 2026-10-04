namespace GrpcBidirectionalStreamingChannels.Server.Processing
{
    public class OrderProcessor
    {
        public async Task<OrderResponse> ProcessAsync(
            OrderRequest request,
            string workerId,
            CancellationToken cancellationToken)
        {
            Console.WriteLine(
                $"[{workerId}] Processing {request.OrderId}");

            await Task.Delay(
                3000,
                cancellationToken);

            return new OrderResponse
            {
                OrderId = request.OrderId,
                Status = OrderStatus.Completed,
                Message =
                    $"Order {request.OrderId} completed",
                ServerTime =
                    DateTime.UtcNow.ToString("O"),
                WorkerId = workerId
            };
        }
    }
}
