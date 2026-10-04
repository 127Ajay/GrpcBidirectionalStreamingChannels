using Grpc.Core;
using GrpcBidirectionalStreamingChannels.Server.Processing;
using System.Threading.Channels;

namespace GrpcBidirectionalStreamingChannels.Server.Services
{
    public class OrderStreamingService
    : OrderStreaming.OrderStreamingBase
    {
        private readonly OrderProcessor _processor;

        public OrderStreamingService(
            OrderProcessor processor)
        {
            _processor = processor;
        }

        public override async Task ProcessOrders(
            IAsyncStreamReader<OrderRequest> requestStream,
            IServerStreamWriter<OrderResponse> responseStream,
            ServerCallContext context)
        {
            var cancellationToken =
                context.CancellationToken;

            var requestChannel =
                Channel.CreateBounded<OrderRequest>(
                    new BoundedChannelOptions(100)
                    {
                        FullMode =
                            BoundedChannelFullMode.Wait,

                        SingleWriter = true,

                        SingleReader = false
                    });

            var responseChannel =
                Channel.CreateBounded<OrderResponse>(
                    new BoundedChannelOptions(100)
                    {
                        FullMode =
                            BoundedChannelFullMode.Wait,

                        SingleWriter = false,

                        SingleReader = true
                    });

            Console.WriteLine(
                "Client connected.");

            var requestReaderTask =
                ReadRequestsAsync(
                    requestStream,
                    requestChannel.Writer,
                    cancellationToken);

            var workerTasks =
                StartWorkers(
                    requestChannel.Reader,
                    responseChannel.Writer,
                    cancellationToken);

            var responseWriterTask =
                WriteResponsesAsync(
                    responseChannel.Reader,
                    responseStream,
                    cancellationToken);

            await requestReaderTask;

            requestChannel.Writer.TryComplete();

            await Task.WhenAll(workerTasks);

            responseChannel.Writer.TryComplete();

            await responseWriterTask;

            Console.WriteLine(
                "Client disconnected.");
        }

        private static async Task ReadRequestsAsync(
            IAsyncStreamReader<OrderRequest> requestStream,
            ChannelWriter<OrderRequest> writer,
            CancellationToken cancellationToken)
        {
            try
            {
                await foreach (
                    var request in requestStream.ReadAllAsync(
                        cancellationToken))
                {
                    Console.WriteLine(
                        $"[READER] Received {request.OrderId}");

                    await writer.WriteAsync(
                        request,
                        cancellationToken);
                }
            }
            finally
            {
                writer.TryComplete();
            }
        }

        private List<Task> StartWorkers(
            ChannelReader<OrderRequest> reader,
            ChannelWriter<OrderResponse> responseWriter,
            CancellationToken cancellationToken)
        {
            const int workerCount = 3;

            var workers = new List<Task>();

            for (int i = 1; i <= workerCount; i++)
            {
                var workerId = $"Worker-{i}";

                workers.Add(
                    ProcessWorkerAsync(
                        workerId,
                        reader,
                        responseWriter,
                        cancellationToken));
            }

            return workers;
        }

        private async Task ProcessWorkerAsync(
                string workerId,
                ChannelReader<OrderRequest> reader,
                ChannelWriter<OrderResponse> responseWriter,
                CancellationToken cancellationToken)
        {
            Console.WriteLine(
                $"[{workerId}] Started.");

            await foreach (
                var request in reader.ReadAllAsync(
                    cancellationToken))
            {
                try
                {
                    Console.WriteLine(
                        $"[{workerId}] " +
                        $"Processing {request.OrderId}");

                    var processingResponse =
                        new OrderResponse
                        {
                            OrderId = request.OrderId,
                            Status = OrderStatus.Processing,
                            Message =
                                $"Processing by {workerId}",
                            ServerTime =
                                DateTime.UtcNow.ToString("O"),
                            WorkerId = workerId
                        };

                    await responseWriter.WriteAsync(
                        processingResponse,
                        cancellationToken);

                    var result =
                        await _processor.ProcessAsync(
                            request,
                            workerId,
                            cancellationToken);

                    await responseWriter.WriteAsync(
                        result,
                        cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    await responseWriter.WriteAsync(
                        new OrderResponse
                        {
                            OrderId = request.OrderId,
                            Status = OrderStatus.Failed,
                            Message = ex.Message,
                            ServerTime =
                                DateTime.UtcNow.ToString("O"),
                            WorkerId = workerId
                        },
                        cancellationToken);
                }
            }

            Console.WriteLine(
                $"[{workerId}] Stopped.");
        }

        private static async Task WriteResponsesAsync(
                ChannelReader<OrderResponse> reader,
                IServerStreamWriter<OrderResponse> responseStream,
                CancellationToken cancellationToken)
        {
            await foreach (
                var response in reader.ReadAllAsync(
                    cancellationToken))
            {
                Console.WriteLine(
                    $"[RESPONSE WRITER] " +
                    $"{response.OrderId} - " +
                    $"{response.Status} - " +
                    $"{response.WorkerId}");

                await responseStream.WriteAsync(
                    response);
            }
        }
    }
}
