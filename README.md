# gRPC Bidirectional Streaming with Channels and Concurrent Workers

A practical **gRPC Bidirectional Streaming** application built with **C#
and .NET 9**.

This project extends the basic bidirectional streaming model by
introducing:

-   `System.Threading.Channels`
-   Producer/consumer architecture
-   Multiple concurrent workers
-   Request and response channels
-   Bounded channels and backpressure
-   Correlation using `OrderId`
-   Cancellation and graceful shutdown
-   Separation of network communication from business processing

The application simulates an **Order Processing Service** where a client
continuously sends orders to a gRPC server while the server processes
those orders concurrently and streams status updates back to the client.

------------------------------------------------------------------------

## Table of Contents

-   [1. Project Overview](#1-project-overview)
-   [2. What Problem Does This Project
    Solve?](#2-what-problem-does-this-project-solve)
-   [3. Technologies Used](#3-technologies-used)
-   [4. Architecture](#4-architecture)
-   [5. Request Processing Flow](#5-request-processing-flow)
-   [6. Response Processing Flow](#6-response-processing-flow)
-   [7. Project Structure](#7-project-structure)
-   [8. Prerequisites](#8-prerequisites)
-   [9. Step-by-Step Project Creation](#9-step-by-step-project-creation)
    -   [9.1 Create the Solution](#91-create-the-solution)
    -   [9.2 Create the Server](#92-create-the-server)
    -   [9.3 Create the Client](#93-create-the-client)
    -   [9.4 Install Client Packages](#94-install-client-packages)
    -   [9.5 Create the Protobuf
        Contract](#95-create-the-protobuf-contract)
    -   [9.6 Configure the Server
        Project](#96-configure-the-server-project)
    -   [9.7 Configure the Client
        Project](#97-configure-the-client-project)
    -   [9.8 Build the Solution](#98-build-the-solution)
    -   [9.9 Create the Order Processor](#99-create-the-order-processor)
    -   [9.10 Register the Processor](#910-register-the-processor)
    -   [9.11 Implement the Streaming
        Service](#911-implement-the-streaming-service)
    -   [9.12 Implement the Request
        Reader](#912-implement-the-request-reader)
    -   [9.13 Implement the Worker Pool](#913-implement-the-worker-pool)
    -   [9.14 Implement the Response
        Writer](#914-implement-the-response-writer)
    -   [9.15 Configure the Server](#915-configure-the-server)
    -   [9.16 Implement the Client](#916-implement-the-client)
    -   [9.17 Run the Server](#917-run-the-server)
    -   [9.18 Run the Client](#918-run-the-client)
-   [10. Understanding the Protobuf
    Contract](#10-understanding-the-protobuf-contract)
-   [11. Understanding `Channel<T>`](#11-understanding-channelt)
-   [12. Why Separate Request Reading from
    Processing?](#12-why-separate-request-reading-from-processing)
-   [13. Why Use Multiple Workers?](#13-why-use-multiple-workers)
-   [14. Why Use a Response Channel?](#14-why-use-a-response-channel)
-   [15. Bounded Channels and
    Backpressure](#15-bounded-channels-and-backpressure)
-   [16. Correlation and Response
    Ordering](#16-correlation-and-response-ordering)
-   [17. Cancellation and Graceful
    Shutdown](#17-cancellation-and-graceful-shutdown)
-   [18. Complete Architecture](#18-complete-architecture)
-   [19. What We Learned](#19-what-we-learned)
-   [20. Important Design
    Considerations](#20-important-design-considerations)
-   [21. Final Takeaways](#21-final-takeaways)

------------------------------------------------------------------------

# 1. Project Overview

The project demonstrates a production-oriented pattern for handling
**long-lived gRPC bidirectional streams**.

The client and server communicate over a single gRPC connection:

``` text
                    ONE gRPC CONNECTION

       CLIENT                                  SERVER
         │                                       │
         │────── OrderRequest ─────────────────►│
         │────── OrderRequest ─────────────────►│
         │────── OrderRequest ─────────────────►│
         │                                       │
         │◄───── OrderResponse ─────────────────│
         │◄───── OrderResponse ─────────────────│
         │                                       │
         │────── OrderRequest ─────────────────►│
         │                                       │
         │◄───── OrderResponse ─────────────────│
         │◄───── OrderResponse ─────────────────│
```

The important architectural improvement is that **receiving a request is
decoupled from processing the request**.

Instead of:

``` text
gRPC Request
     ↓
Process
     ↓
Response
```

the server uses:

``` text
gRPC Request
     ↓
Request Channel
     ↓
Worker Pool
     ↓
Response Channel
     ↓
gRPC Response
```

This allows requests to be received continuously while multiple workers
process them concurrently.

------------------------------------------------------------------------

# 2. What Problem Does This Project Solve?

A simple gRPC streaming implementation can process requests like this:

``` csharp
await foreach (var request in requestStream.ReadAllAsync())
{
    await ProcessRequest(request);
}
```

This creates a coupling between:

1.  Receiving the network message
2.  Processing the message
3.  Sending the response

If processing takes three seconds, the same processing loop cannot
immediately move on to the next request.

For example:

``` text
ORD-001
   ↓
3 seconds
   ↓
ORD-002
   ↓
3 seconds
   ↓
ORD-003
```

The client might have already sent all three orders, but the server
processes them sequentially.

This project introduces a channel between receiving and processing:

``` text
ORD-001 ──┐
ORD-002 ──┤
ORD-003 ──┤
ORD-004 ──┤
ORD-005 ──┘
           │
           ▼
    Request Channel
           │
     ┌─────┼─────┐
     ▼     ▼     ▼
   Worker Worker Worker
      1      2      3
```

Now multiple requests can be processed concurrently.

------------------------------------------------------------------------

# 3. Technologies Used

  Technology                    Purpose
  ----------------------------- ---------------------------------------
  C#                            Application programming language
  .NET 9                        Runtime and application framework
  ASP.NET Core                  gRPC server
  gRPC                          Client/server communication
  HTTP/2                        Transport used by gRPC
  Protocol Buffers              Service contract
  `Grpc.Net.Client`             .NET gRPC client
  `Google.Protobuf`             Protocol Buffer support
  `Grpc.Tools`                  C# code generation from `.proto`
  `System.Threading.Channels`   Asynchronous producer/consumer queues

------------------------------------------------------------------------

# 4. Architecture

The server is divided into four major responsibilities.

``` text
                         gRPC CLIENT
                              │
                              │
                       Request Stream
                              │
                              ▼
                    ┌───────────────────┐
                    │   Request Reader  │
                    │                   │
                    │ Reads gRPC only   │
                    └─────────┬─────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │  Request Channel  │
                    │                   │
                    │ Producer/Consumer │
                    │       Queue       │
                    └─────────┬─────────┘
                              │
               ┌──────────────┼──────────────┐
               │              │              │
               ▼              ▼              ▼
          ┌─────────┐    ┌─────────┐    ┌─────────┐
          │Worker-1 │    │Worker-2 │    │Worker-3 │
          └────┬────┘    └────┬────┘    └────┬────┘
               │              │              │
               └──────────────┼──────────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │ Response Channel  │
                    │                   │
                    │ Worker Results    │
                    └─────────┬─────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │ Response Writer   │
                    │                   │
                    │ Single gRPC       │
                    │ response writer   │
                    └─────────┬─────────┘
                              │
                              │
                       Response Stream
                              │
                              ▼
                         gRPC CLIENT
```

Each component has one primary responsibility.

### Request Reader

Reads messages from the gRPC request stream.

### Request Channel

Buffers requests waiting for processing.

### Workers

Consume requests from the channel and process them concurrently.

### Response Channel

Collects results generated by the workers.

### Response Writer

Reads results from the response channel and writes them to the gRPC
response stream.

------------------------------------------------------------------------

# 5. Request Processing Flow

The complete request flow is:

``` text
Client
  │
  │ OrderRequest
  ▼
gRPC Request Stream
  │
  ▼
ReadRequestsAsync()
  │
  ▼
Request Channel
  │
  ├───────────────┐
  │               │
  ▼               ▼
Worker-1        Worker-2        Worker-3
  │               │               │
  └───────────────┼───────────────┘
                  │
                  ▼
            OrderProcessor
```

The request reader does not perform business processing.

It simply receives requests and puts them into the channel.

------------------------------------------------------------------------

# 6. Response Processing Flow

The response flow works in the opposite direction:

``` text
Worker-1 ──┐
Worker-2 ──┼──► Response Channel
Worker-3 ──┘
                 │
                 ▼
         WriteResponsesAsync()
                 │
                 ▼
          gRPC Response Stream
                 │
                 ▼
               Client
```

The workers do not directly write to the gRPC response stream.

Instead, they publish responses into the response channel.

A single response writer owns the gRPC response stream.

This keeps network output separate from concurrent processing.

------------------------------------------------------------------------

# 7. Project Structure

The completed project has the following structure:

``` text
GrpcBidirectionalStreamingChannels/
│
├── GrpcBidirectionalStreamingChannels.sln
│
├── GrpcBidirectionalStreamingChannels.Server/
│   │
│   ├── Protos/
│   │   └── order.proto
│   │
│   ├── Services/
│   │   └── OrderStreamingService.cs
│   │
│   ├── Processing/
│   │   └── OrderProcessor.cs
│   │
│   ├── Program.cs
│   └── GrpcBidirectionalStreamingChannels.Server.csproj
│
└── GrpcBidirectionalStreamingChannels.Client/
    │
    ├── Program.cs
    └── GrpcBidirectionalStreamingChannels.Client.csproj
```

------------------------------------------------------------------------

# 8. Prerequisites

Install:

-   .NET 9 SDK
-   Visual Studio, JetBrains Rider, or VS Code
-   Git

Verify .NET:

``` bash
dotnet --version
```

Check installed SDKs:

``` bash
dotnet --list-sdks
```

A .NET 9 SDK should be available:

``` text
9.0.xxx
```

------------------------------------------------------------------------

# 9. Step-by-Step Project Creation

## 9.1 Create the Solution

Create a new directory:

``` bash
mkdir GrpcBidirectionalStreamingChannels
cd GrpcBidirectionalStreamingChannels
```

Create the solution:

``` bash
dotnet new sln -n GrpcBidirectionalStreamingChannels
```

Initial structure:

``` text
GrpcBidirectionalStreamingChannels/
└── GrpcBidirectionalStreamingChannels.sln
```

------------------------------------------------------------------------

## 9.2 Create the Server

Create the ASP.NET Core gRPC project:

``` bash
dotnet new grpc -n GrpcBidirectionalStreamingChannels.Server
```

Add it to the solution:

``` bash
dotnet sln add GrpcBidirectionalStreamingChannels.Server/GrpcBidirectionalStreamingChannels.Server.csproj
```

Delete the default gRPC example:

``` text
Protos/greet.proto
Services/GreeterService.cs
```

------------------------------------------------------------------------

## 9.3 Create the Client

Create the console application:

``` bash
dotnet new console -n GrpcBidirectionalStreamingChannels.Client
```

Add it to the solution:

``` bash
dotnet sln add GrpcBidirectionalStreamingChannels.Client/GrpcBidirectionalStreamingChannels.Client.csproj
```

The solution now contains:

``` text
GrpcBidirectionalStreamingChannels/
│
├── GrpcBidirectionalStreamingChannels.sln
│
├── GrpcBidirectionalStreamingChannels.Server/
│
└── GrpcBidirectionalStreamingChannels.Client/
```

------------------------------------------------------------------------

## 9.4 Install Client Packages

Navigate to the client:

``` bash
cd GrpcBidirectionalStreamingChannels.Client
```

Install:

``` bash
dotnet add package Grpc.Net.Client
```

``` bash
dotnet add package Google.Protobuf
```

``` bash
dotnet add package Grpc.Tools
```

Return to the root:

``` bash
cd ..
```

------------------------------------------------------------------------

## 9.5 Create the Protobuf Contract

Create:

``` text
GrpcBidirectionalStreamingChannels.Server/Protos/order.proto
```

Use:

``` protobuf
syntax = "proto3";

option csharp_namespace = "GrpcBidirectionalStreamingChannels";

package orders;

service OrderStreaming {

    rpc ProcessOrders(stream OrderRequest)
        returns (stream OrderResponse);
}

message OrderRequest {

    string order_id = 1;

    OrderAction action = 2;

    string product_name = 3;

    int32 quantity = 4;
}

message OrderResponse {

    string order_id = 1;

    OrderStatus status = 2;

    string message = 3;

    string server_time = 4;

    string worker_id = 5;
}

enum OrderAction {

    UNKNOWN = 0;

    CREATE = 1;

    CANCEL = 2;

    STATUS = 3;
}

enum OrderStatus {

    RECEIVED = 0;

    PROCESSING = 1;

    COMPLETED = 2;

    CANCELLED = 3;

    FAILED = 4;
}
```

The key RPC definition is:

``` protobuf
rpc ProcessOrders(stream OrderRequest)
    returns (stream OrderResponse);
```

The first `stream` means the client can send multiple requests.

The second `stream` means the server can send multiple responses.

Therefore this is a bidirectional streaming RPC.

------------------------------------------------------------------------

## 9.6 Configure the Server Project

Open:

``` text
GrpcBidirectionalStreamingChannels.Server.csproj
```

Ensure the protobuf contract is configured for server code generation:

``` xml
<ItemGroup>
  <Protobuf Include="Protos\order.proto"
            GrpcServices="Server" />
</ItemGroup>
```

------------------------------------------------------------------------

## 9.7 Configure the Client Project

Open:

``` text
GrpcBidirectionalStreamingChannels.Client.csproj
```

Add:

``` xml
<ItemGroup>
  <Protobuf Include="..\GrpcBidirectionalStreamingChannels.Server\Protos\order.proto"
            GrpcServices="Client"
            Link="Protos\order.proto" />
</ItemGroup>
```

Both projects now generate C# types from the same protobuf contract.

``` text
                    order.proto
                         │
                ┌────────┴────────┐
                ▼                 ▼
             Server             Client
                │                 │
         Server generated   Client generated
             classes             classes
```

------------------------------------------------------------------------

## 9.8 Build the Solution

From the solution root:

``` bash
dotnet build
```

Expected:

``` text
Build succeeded.
```

If the protobuf contract does not compile, resolve that before
continuing.

------------------------------------------------------------------------

## 9.9 Create the Order Processor

Create:

``` text
GrpcBidirectionalStreamingChannels.Server/Processing/OrderProcessor.cs
```

Add:

``` csharp
using GrpcBidirectionalStreamingChannels;

namespace GrpcBidirectionalStreamingChannels.Server.Processing;

public class OrderProcessor
{
    public async Task<OrderResponse> ProcessAsync(
        OrderRequest request,
        string workerId,
        CancellationToken cancellationToken)
    {
        Console.WriteLine(
            $"[{workerId}] Processing {request.OrderId}");

        // Simulate a long-running operation.
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
```

The three-second delay is intentional. It makes concurrent processing
easy to observe.

In a real application this could represent:

-   Database processing
-   External API calls
-   Business-rule execution
-   File processing
-   Another service call
-   Long-running calculations

------------------------------------------------------------------------

## 9.10 Register the Processor

Update `Program.cs`:

``` csharp
using GrpcBidirectionalStreamingChannels.Server.Processing;
using GrpcBidirectionalStreamingChannels.Server.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc();

builder.Services.AddSingleton<OrderProcessor>();

var app = builder.Build();

app.MapGrpcService<OrderStreamingService>();

app.MapGet("/", () =>
    "gRPC Bidirectional Streaming with Channels");

app.Run();
```

The important registration is:

``` csharp
builder.Services.AddSingleton<OrderProcessor>();
```

This allows the gRPC service to receive `OrderProcessor` through
dependency injection.

------------------------------------------------------------------------

## 9.11 Implement the Streaming Service

Create:

``` text
GrpcBidirectionalStreamingChannels.Server/Services/OrderStreamingService.cs
```

Start with:

``` csharp
using System.Threading.Channels;
using Grpc.Core;
using GrpcBidirectionalStreamingChannels.Server.Processing;

namespace GrpcBidirectionalStreamingChannels.Server.Services;

public class OrderStreamingService
    : OrderStreaming.OrderStreamingBase
{
    private readonly OrderProcessor _processor;

    public OrderStreamingService(
        OrderProcessor processor)
    {
        _processor = processor;
    }
}
```

The service receives the processor through dependency injection.

The actual streaming method creates channels for the current streaming
session:

``` csharp
public override async Task ProcessOrders(
    IAsyncStreamReader<OrderRequest> requestStream,
    IServerStreamWriter<OrderResponse> responseStream,
    ServerCallContext context)
{
    var cancellationToken =
        context.CancellationToken;

    var requestChannel =
        Channel.CreateUnbounded<OrderRequest>();

    var responseChannel =
        Channel.CreateUnbounded<OrderResponse>();

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
```

### Why are the channels created inside `ProcessOrders`?

Because a gRPC streaming call represents a client session.

Creating the channels inside the method means each streaming call gets
its own:

-   Request channel
-   Response channel
-   Worker group

This prevents requests from different clients from accidentally sharing
the same queue.

------------------------------------------------------------------------

## 9.12 Implement the Request Reader

Add:

``` csharp
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
```

The request reader has one responsibility:

> Receive messages from gRPC and put them into the request channel.

It does not process orders.

The flow is:

``` text
Client
  │
  ▼
gRPC Request Stream
  │
  ▼
ReadRequestsAsync
  │
  ▼
Request Channel
```

This separation is important because the request reader can continue
receiving messages while workers process previous requests.

------------------------------------------------------------------------

## 9.13 Implement the Worker Pool

Create three workers:

``` csharp
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
```

This creates:

``` text
Worker-1
Worker-2
Worker-3
```

All three workers consume from the same request channel.

### Worker implementation

``` csharp
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

            await responseWriter.WriteAsync(
                new OrderResponse
                {
                    OrderId = request.OrderId,
                    Status = OrderStatus.Processing,
                    Message =
                        $"Processing by {workerId}",
                    ServerTime =
                        DateTime.UtcNow.ToString("O"),
                    WorkerId = workerId
                },
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
```

Each worker:

1.  Waits for a request.
2.  Takes one request from the channel.
3.  Sends a `PROCESSING` response.
4.  Processes the order.
5.  Sends a `COMPLETED` response.
6.  Goes back to the channel for another request.

------------------------------------------------------------------------

## 9.14 Implement the Response Writer

Add:

``` csharp
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
```

This creates a single path from the response channel to the gRPC client:

``` text
Worker-1 ──┐
Worker-2 ──┼──► Response Channel
Worker-3 ──┘
                 │
                 ▼
        WriteResponsesAsync()
                 │
                 ▼
          gRPC Response Stream
                 │
                 ▼
               Client
```

The response writer is the only component that writes to the gRPC
response stream.

------------------------------------------------------------------------

## 9.15 Configure the Server

The complete `Program.cs`:

``` csharp
using GrpcBidirectionalStreamingChannels.Server.Processing;
using GrpcBidirectionalStreamingChannels.Server.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc();

builder.Services.AddSingleton<OrderProcessor>();

var app = builder.Build();

app.MapGrpcService<OrderStreamingService>();

app.MapGet("/", () =>
    "gRPC Bidirectional Streaming with Channels");

app.Run();
```

Run the server:

``` bash
cd GrpcBidirectionalStreamingChannels.Server
dotnet run
```

You should see:

``` text
Now listening on: https://localhost:7001
```

The port may differ depending on the generated ASP.NET Core
configuration.

------------------------------------------------------------------------

## 9.16 Implement the Client

Open:

``` text
GrpcBidirectionalStreamingChannels.Client/Program.cs
```

Use:

``` csharp
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
```

The client starts two independent operations:

``` csharp
var sendTask = SendOrdersAsync(
    call.RequestStream);

var receiveTask = ReceiveResponsesAsync(
    call.ResponseStream);
```

Then waits for both:

``` csharp
await Task.WhenAll(
    sendTask,
    receiveTask);
```

------------------------------------------------------------------------

## 9.17 Run the Server

Open Terminal 1:

``` bash
cd GrpcBidirectionalStreamingChannels.Server
dotnet run
```

Keep this process running.

------------------------------------------------------------------------

## 9.18 Run the Client

Open Terminal 2:

``` bash
cd GrpcBidirectionalStreamingChannels.Client
dotnet run
```

The client connects to the server and starts sending orders.

------------------------------------------------------------------------

# 10. Understanding the Protobuf Contract

The service is defined as:

``` protobuf
service OrderStreaming {

    rpc ProcessOrders(stream OrderRequest)
        returns (stream OrderResponse);
}
```

This defines a bidirectional streaming RPC.

The client can send:

``` text
OrderRequest
OrderRequest
OrderRequest
...
```

The server can send:

``` text
OrderResponse
OrderResponse
OrderResponse
...
```

Both streams belong to the same RPC call.

The important distinction is:

``` text
Unary:

Request ─────►
Response ◄────


Bidirectional:

Request ─────►
Request ─────►
Response ◄────
Request ─────►
Response ◄────
Response ◄────
```

------------------------------------------------------------------------

# 11. Understanding `Channel<T>`

`System.Threading.Channels` provides an asynchronous producer/consumer
mechanism.

A channel can be visualized as a queue:

``` text
Producer
   │
   ▼
┌──────────────────────┐
│      Channel<T>      │
│                      │
│ Request 1            │
│ Request 2            │
│ Request 3            │
└──────────┬───────────┘
           │
           ▼
        Consumer
```

In this project:

``` text
Request Reader = Producer
Workers        = Consumers
```

The request reader writes:

``` csharp
await writer.WriteAsync(
    request,
    cancellationToken);
```

Workers read:

``` csharp
await foreach (
    var request in reader.ReadAllAsync(
        cancellationToken))
{
    ...
}
```

This allows the producer and consumers to operate independently.

------------------------------------------------------------------------

# 12. Why Separate Request Reading from Processing?

Without a channel:

``` text
Read Request
    ↓
Process Request
    ↓
Read Request
    ↓
Process Request
```

With a channel:

``` text
Read Request
    ↓
Write to Channel
    ↓
Read Request
    ↓
Write to Channel
    ↓
Read Request
```

Meanwhile:

``` text
Channel
   │
   ├── Worker 1
   ├── Worker 2
   └── Worker 3
```

The network-facing operation is therefore independent of business
processing.

This is useful when processing is slower than message arrival.

------------------------------------------------------------------------

# 13. Why Use Multiple Workers?

One worker:

``` text
Request 1 → Worker → 3 sec
Request 2 → Worker → 3 sec
Request 3 → Worker → 3 sec
```

Three workers:

``` text
Request 1 → Worker 1 ──┐
Request 2 → Worker 2 ──┼── 3 sec
Request 3 → Worker 3 ──┘
```

Multiple workers increase processing concurrency.

With three workers:

``` text
             Request Channel
                    │
        ┌───────────┼───────────┐
        ▼           ▼           ▼
     Worker-1    Worker-2    Worker-3
        │           │           │
        ▼           ▼           ▼
     Request      Request      Request
```

The number of workers should not be chosen arbitrarily in a real system.

It depends on:

-   CPU requirements
-   Database capacity
-   External service limits
-   Expected throughput
-   Message ordering requirements
-   Memory consumption
-   Downstream bottlenecks

------------------------------------------------------------------------

# 14. Why Use a Response Channel?

Multiple workers can finish at different times:

``` text
Worker-1 ──┐
Worker-2 ──┼──► Response Channel
Worker-3 ──┘
```

The response channel acts as a synchronization boundary.

Workers only need to produce results.

The response writer is responsible for network output.

``` text
Workers
   ↓
Response Channel
   ↓
Response Writer
   ↓
gRPC
```

This separation prevents business-processing code from being tightly
coupled to the gRPC network stream.

------------------------------------------------------------------------

# 15. Bounded Channels and Backpressure

The initial implementation uses:

``` csharp
Channel.CreateUnbounded<OrderRequest>();
```

An unbounded channel can grow as large as memory permits.

Consider:

``` text
Client
   │
   │ 1000 requests/sec
   ▼
Request Channel
   │
   │ 3 workers
   ▼
Processing
```

If workers can process only 100 requests/sec, the queue can continuously
grow.

That can eventually create memory pressure.

A bounded channel limits capacity.

For example:

``` csharp
var requestChannel =
    Channel.CreateBounded<OrderRequest>(
        new BoundedChannelOptions(100)
        {
            FullMode =
                BoundedChannelFullMode.Wait,

            SingleWriter = true,

            SingleReader = false
        });
```

The channel now has a maximum capacity of 100.

When it is full:

``` text
Request
Request
Request
...
Request
────────────
Channel FULL
```

the producer waits:

``` csharp
await writer.WriteAsync(
    request,
    cancellationToken);
```

until space becomes available.

This is called **backpressure**.

------------------------------------------------------------------------

## Response Channel Configuration

The response channel can be configured as:

``` csharp
var responseChannel =
    Channel.CreateBounded<OrderResponse>(
        new BoundedChannelOptions(100)
        {
            FullMode =
                BoundedChannelFullMode.Wait,

            SingleWriter = false,

            SingleReader = true
        });
```

The characteristics are:

### Request Channel

``` text
Single Writer
Multiple Readers
```

Because there is one request reader and multiple workers.

### Response Channel

``` text
Multiple Writers
Single Reader
```

Because multiple workers produce responses and one response writer
consumes them.

------------------------------------------------------------------------

# 16. Correlation and Response Ordering

Concurrent processing means that response order cannot automatically be
assumed to match request order.

Suppose the client sends:

``` text
ORD-001
ORD-002
ORD-003
```

The workers could process:

``` text
Worker-1 → ORD-001
Worker-2 → ORD-002
Worker-3 → ORD-003
```

If processing durations differ:

``` text
ORD-001 = 3 seconds
ORD-002 = 1 second
ORD-003 = 2 seconds
```

the responses could arrive as:

``` text
ORD-002 COMPLETED
ORD-003 COMPLETED
ORD-001 COMPLETED
```

Therefore the response contains:

``` protobuf
string order_id = 1;
```

The client uses `OrderId` to associate a response with the original
request.

This is a fundamental concept in distributed systems.

The same principle appears in:

-   Kafka
-   RabbitMQ
-   Azure Service Bus
-   Event-driven systems
-   Asynchronous APIs
-   Message queues
-   Distributed workflows

------------------------------------------------------------------------

# 17. Cancellation and Graceful Shutdown

The server receives:

``` csharp
ServerCallContext context
```

The context provides:

``` csharp
context.CancellationToken
```

That token is passed to:

``` csharp
requestStream.ReadAllAsync(
    cancellationToken)
```

and:

``` csharp
writer.WriteAsync(
    request,
    cancellationToken)
```

and:

``` csharp
Task.Delay(
    3000,
    cancellationToken)
```

This means that if the streaming call is cancelled, the server can stop
its operations.

The worker explicitly handles:

``` csharp
catch (OperationCanceledException)
{
    break;
}
```

This allows workers to exit cleanly.

------------------------------------------------------------------------

## Channel Completion

The request channel is completed after the request stream finishes:

``` csharp
requestChannel.Writer.TryComplete();
```

Workers then eventually finish reading all remaining requests and exit
when the channel is empty.

After all workers finish:

``` csharp
await Task.WhenAll(workerTasks);
```

the response channel is completed:

``` csharp
responseChannel.Writer.TryComplete();
```

The response writer then finishes after consuming the remaining
responses.

The overall lifecycle is:

``` text
Client finishes requests
        │
        ▼
Request Reader completes
        │
        ▼
Request Channel completes
        │
        ▼
Workers finish queued requests
        │
        ▼
All Workers Complete
        │
        ▼
Response Channel completes
        │
        ▼
Response Writer completes
        │
        ▼
Streaming RPC completes
```

------------------------------------------------------------------------

# 18. Complete Architecture

The complete application can be represented as:

``` text
                              CLIENT
                                │
                                │
                         OrderRequest Stream
                                │
                                ▼
                     ┌────────────────────┐
                     │   Request Reader   │
                     │                    │
                     │ Read gRPC messages │
                     └─────────┬──────────┘
                               │
                               ▼
                     ┌────────────────────┐
                     │  Request Channel   │
                     │                    │
                     │ Bounded / Async    │
                     │ Producer/Consumer  │
                     └─────────┬──────────┘
                               │
                 ┌─────────────┼─────────────┐
                 │             │             │
                 ▼             ▼             ▼
            ┌─────────┐   ┌─────────┐   ┌─────────┐
            │Worker-1 │   │Worker-2 │   │Worker-3 │
            └────┬────┘   └────┬────┘   └────┬────┘
                 │             │             │
                 │             │             │
                 └─────────────┼─────────────┘
                               │
                               ▼
                     ┌────────────────────┐
                     │ Response Channel   │
                     │                    │
                     │ Worker Results     │
                     └─────────┬──────────┘
                               │
                               ▼
                     ┌────────────────────┐
                     │  Response Writer   │
                     │                    │
                     │ Single gRPC Writer │
                     └─────────┬──────────┘
                               │
                               │
                        OrderResponse Stream
                               │
                               ▼
                             CLIENT
```

The architecture separates:

``` text
Communication
     ↓
Queueing
     ↓
Processing
     ↓
Response Queueing
     ↓
Communication
```

------------------------------------------------------------------------

# 19. What We Learned

This project demonstrates several important concepts.

## 19.1 Bidirectional gRPC Streaming

We learned how to define:

``` protobuf
rpc ProcessOrders(stream OrderRequest)
    returns (stream OrderResponse);
```

and maintain a long-lived communication channel between client and
server.

------------------------------------------------------------------------

## 19.2 Independent Request and Response Streams

The client can independently:

``` csharp
call.RequestStream
```

and:

``` csharp
call.ResponseStream
```

This allows sending and receiving concurrently.

------------------------------------------------------------------------

## 19.3 `IAsyncStreamReader<T>`

Used to asynchronously consume streaming messages:

``` csharp
await foreach (
    var request in requestStream.ReadAllAsync())
{
}
```

------------------------------------------------------------------------

## 19.4 `IClientStreamWriter<T>`

Used by the client to send streaming requests:

``` csharp
await requestStream.WriteAsync(
    request);
```

------------------------------------------------------------------------

## 19.5 `IServerStreamWriter<T>`

Used by the server to send streaming responses:

``` csharp
await responseStream.WriteAsync(
    response);
```

------------------------------------------------------------------------

## 19.6 `Channel<T>`

We learned how channels decouple:

``` text
Producer
   ↓
Channel
   ↓
Consumer
```

In this project:

``` text
gRPC Request Reader
        ↓
Request Channel
        ↓
Workers
```

------------------------------------------------------------------------

## 19.7 Producer/Consumer Pattern

The request reader is the producer.

The workers are consumers.

``` text
Producer
   │
   ▼
Channel
   │
   ├── Consumer
   ├── Consumer
   └── Consumer
```

This pattern is widely applicable in asynchronous .NET applications.

------------------------------------------------------------------------

## 19.8 Concurrent Processing

Multiple workers can process different requests at the same time.

``` text
Worker-1 → ORD-001
Worker-2 → ORD-002
Worker-3 → ORD-003
```

This improves throughput when processing operations can run
independently.

------------------------------------------------------------------------

## 19.9 Backpressure

Bounded channels prevent an unlimited queue from consuming memory.

``` text
Fast Producer
      ↓
Bounded Channel
      ↓
Slower Consumers
```

When the channel is full, the producer waits.

------------------------------------------------------------------------

## 19.10 Correlation IDs

`OrderId` allows the client to associate responses with their original
requests.

This becomes essential when processing is concurrent and responses can
arrive out of order.

------------------------------------------------------------------------

## 19.11 Cancellation

We learned how to propagate:

``` csharp
context.CancellationToken
```

through:

-   gRPC stream reading
-   channel writes
-   channel reads
-   worker processing
-   long-running operations

This is important for graceful shutdown and client disconnection.

------------------------------------------------------------------------

## 19.12 Graceful Channel Completion

The producer completes the request channel:

``` csharp
requestChannel.Writer.TryComplete();
```

Workers finish remaining messages.

After workers complete, the response channel is completed:

``` csharp
responseChannel.Writer.TryComplete();
```

This creates a controlled shutdown sequence instead of abruptly
terminating the processing pipeline.

------------------------------------------------------------------------

# 20. Important Design Considerations

The implementation is intentionally designed to demonstrate concurrency,
but several decisions need more thought in a production system.

## Worker Count

Three workers are used for demonstration:

``` csharp
const int workerCount = 3;
```

In production, worker count should be based on workload and downstream
capacity.

More workers do not automatically mean better performance.

If the database can handle only 20 concurrent operations, creating 100
workers may make the system worse.

------------------------------------------------------------------------

## Ordering

Concurrent workers mean responses may arrive out of order.

If strict ordering is required, this architecture needs additional
coordination.

Possible ordering models include:

``` text
Global ordering
```

or:

``` text
Ordering per customer
```

or:

``` text
Ordering per partition/key
```

or:

``` text
No ordering requirement
```

The correct approach depends on the business requirement.

------------------------------------------------------------------------

## Channel Capacity

Unbounded channels are simple but potentially dangerous under sustained
load.

Bounded channels provide a mechanism for backpressure:

``` csharp
Channel.CreateBounded<T>()
```

The capacity should be selected based on expected traffic, processing
rate, and memory constraints.

------------------------------------------------------------------------

## Error Handling

Workers catch processing exceptions and publish a failed response:

``` csharp
catch (Exception ex)
{
    await responseWriter.WriteAsync(
        new OrderResponse
        {
            OrderId = request.OrderId,
            Status = OrderStatus.Failed,
            Message = ex.Message
        });
}
```

A production system would typically distinguish between:

-   Validation errors
-   Business errors
-   Transient errors
-   Permanent errors
-   Infrastructure failures
-   Cancellation

Different categories may require different handling strategies.

------------------------------------------------------------------------

## Streaming Lifetime

A bidirectional streaming call can remain open for a long time.

Production implementations should consider:

-   Cancellation
-   Client disconnects
-   Server shutdown
-   Deadlines
-   Network failures
-   Reconnection
-   Authentication
-   Authorization
-   Observability
-   Resource cleanup

------------------------------------------------------------------------

# 21. Final Takeaways

The main lesson from this project is the separation of responsibilities.

The gRPC layer handles communication:

``` text
Client ↔ Server
```

The request channel handles buffering:

``` text
Request Reader → Request Channel
```

Workers handle processing:

``` text
Request Channel → Workers
```

The response channel handles result coordination:

``` text
Workers → Response Channel
```

The response writer handles network output:

``` text
Response Channel → gRPC Client
```

The final mental model is:

``` text
                    NETWORK
                       │
                       ▼
               ┌───────────────┐
               │ Request Reader│
               └───────┬───────┘
                       │
                       ▼
                 REQUEST CHANNEL
                       │
             ┌─────────┼─────────┐
             ▼         ▼         ▼
            W1        W2        W3
             │         │         │
             └─────────┼─────────┘
                       │
                       ▼
                RESPONSE CHANNEL
                       │
                       ▼
               ┌───────────────┐
               │Response Writer│
               └───────┬───────┘
                       │
                       ▼
                    NETWORK
```

The important architectural principle is:

> **Do not make the network stream responsible for business
> processing.**

Instead, use the gRPC stream as the communication boundary and use
channels to decouple communication from processing.

This pattern provides a foundation for building more sophisticated
asynchronous services involving databases, external APIs, message
processing, retries, backpressure, and controlled concurrency.
