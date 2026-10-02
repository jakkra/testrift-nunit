using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using MessagePack;
using NUnit.Framework;

namespace TestRift.NUnit.Tests
{
    [TestFixture]
    [NonParallelizable]
    public sealed class ReliabilityTests
    {
        private string _configPath;

        [SetUp]
        public void SetUp()
        {
            _configPath = Path.Combine(Path.GetTempPath(), $"testrift-reliability-{Guid.NewGuid():N}.yaml");
        }

        [TearDown]
        public void TearDown()
        {
            TRLoggerAttribute.NotifyRunFinished();
            if (File.Exists(_configPath))
                File.Delete(_configPath);
        }

        [Test]
        public async Task FlushAllMessages_DeliversEveryEventAcrossBatchBoundaries()
        {
            const int eventCount = 1201;
            using (var server = new FakeTestRiftServer())
            using (var helper = await StartRunAsync(server))
            {
                for (var i = 0; i < eventCount; i++)
                    helper.QueueLogMessage($"line {i}", "test", nunitTestId: "tc-1");

                await helper.FlushAllMessagesAsync();

                Assert.That(() => server.LogEntryCount, Is.EqualTo(eventCount).After(5000, 50));
                Assert.That(helper.DroppedEventCount, Is.Zero);
            }
        }

        [Test]
        public async Task FlushAllMessages_CountsEventsLostAfterServerCloses()
        {
            using (var server = new FakeTestRiftServer(closeAfterRunStart: true))
            using (var helper = await StartRunAsync(server))
            {
                await server.Closed.WaitAsync(TimeSpan.FromSeconds(5));

                var stopwatch = Stopwatch.StartNew();
                while (helper.DroppedEventCount == 0 && stopwatch.Elapsed < TimeSpan.FromSeconds(5))
                {
                    helper.QueueLogMessage("after close", "test", nunitTestId: "tc-1");
                    await helper.FlushAllMessagesAsync();
                    await Task.Delay(50);
                }

                Assert.That(helper.DroppedEventCount, Is.GreaterThan(0));
            }
        }

        [Test]
        public void NotifyRunStarted_WithUnreachableServer_LetsTestsContinue()
        {
            WriteConfig(UnusedLoopbackUrl());

            var stopwatch = Stopwatch.StartNew();
            Assert.DoesNotThrow(TRLoggerAttribute.NotifyRunStarted);
            Assert.That(stopwatch.Elapsed, Is.LessThan(TimeSpan.FromSeconds(30)));
        }

        [Test]
        public void RunHooks_WithMissingEnvironmentVariable_DisablesReportingWithoutThrowing()
        {
            File.WriteAllText(_configPath, string.Join(
                "\n",
                "serverUrl: http://127.0.0.1:1",
                "runName: ${env:TESTRIFT_RELIABILITY_TEST_UNSET_VARIABLE}",
                "target: test-target",
                "sources:",
                "  test-system:",
                "    branch: main",
                "    revision: test-revision"));

            Assert.DoesNotThrow(() => new RunHooks(_configPath));
            Assert.DoesNotThrow(TRLoggerAttribute.NotifyRunStarted);
        }

        private async Task<WebSocketHelper> StartRunAsync(FakeTestRiftServer server)
        {
            WriteConfig(server.Url);
            var helper = new WebSocketHelper(server.Url);
            await helper.ConnectAsync();
            await helper.SendRunStarted();
            Assert.That(helper.IsRunActive, Is.True);
            return helper;
        }

        private void WriteConfig(string serverUrl)
        {
            File.WriteAllText(_configPath, string.Join(
                "\n",
                $"serverUrl: {serverUrl}",
                "target: test-target",
                "purpose: manual",
                "sources:",
                "  test-system:",
                "    branch: main",
                "    revision: test-revision"));
            ConfigManager.Load(_configPath);
        }

        private static string UnusedLoopbackUrl()
        {
            var reservation = new TcpListener(IPAddress.Loopback, 0);
            reservation.Start();
            var port = ((IPEndPoint)reservation.LocalEndpoint).Port;
            reservation.Stop();
            return $"http://127.0.0.1:{port}";
        }

        private sealed class FakeTestRiftServer : IDisposable
        {
            private static readonly MessagePackSerializerOptions Options = MessagePackSerializerOptions.Standard
                .WithResolver(MessagePack.Resolvers.ContractlessStandardResolver.Instance);

            private readonly HttpListener _listener = new HttpListener();
            private readonly CancellationTokenSource _cts = new CancellationTokenSource();
            private readonly bool _closeAfterRunStart;
            private readonly TaskCompletionSource<bool> _closed =
                new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            private int _logEntryCount;

            public FakeTestRiftServer(bool closeAfterRunStart = false)
            {
                _closeAfterRunStart = closeAfterRunStart;
                Url = UnusedLoopbackUrl();
                _listener.Prefixes.Add(Url + "/");
                _listener.Start();
                _ = Task.Run(AcceptAsync);
            }

            public string Url { get; }

            public Task Closed => _closed.Task;

            public int LogEntryCount => Volatile.Read(ref _logEntryCount);

            private async Task AcceptAsync()
            {
                try
                {
                    while (!_cts.IsCancellationRequested)
                    {
                        var context = await _listener.GetContextAsync();
                        if (!context.Request.IsWebSocketRequest)
                        {
                            context.Response.StatusCode = 404;
                            context.Response.Close();
                            continue;
                        }
                        var webSocketContext = await context.AcceptWebSocketAsync(null);
                        _ = Task.Run(() => HandleAsync(webSocketContext.WebSocket));
                    }
                }
                catch (Exception) when (_cts.IsCancellationRequested)
                {
                }
            }

            private async Task HandleAsync(WebSocket socket)
            {
                var buffer = new byte[64 * 1024];
                using (var message = new MemoryStream())
                {
                    try
                    {
                        while (socket.State == WebSocketState.Open)
                        {
                            var result = await socket.ReceiveAsync(new ArraySegment<byte>(buffer), _cts.Token);
                            if (result.MessageType == WebSocketMessageType.Close)
                                break;

                            message.Write(buffer, 0, result.Count);
                            if (!result.EndOfMessage)
                                continue;

                            var received = MessagePackSerializer.Deserialize<Dictionary<string, object>>(message.ToArray(), Options);
                            message.SetLength(0);
                            var type = Convert.ToInt32(received["t"]);
                            if (type == 8)
                                CountLogEntries(received);
                            if (type != 1)
                                continue;

                            var response = MessagePackSerializer.Serialize(
                                new Dictionary<string, object> { { "t", 2 }, { "r", "run-1" } },
                                Options);
                            await socket.SendAsync(new ArraySegment<byte>(response), WebSocketMessageType.Binary, true, _cts.Token);

                            if (_closeAfterRunStart)
                            {
                                await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "closing", _cts.Token);
                                _closed.TrySetResult(true);
                                return;
                            }
                        }
                    }
                    catch (Exception) when (_cts.IsCancellationRequested)
                    {
                    }
                }
            }

            private void CountLogEntries(Dictionary<string, object> batch)
            {
                foreach (var item in (IEnumerable)batch["ev"])
                {
                    var evt = (IDictionary)item;
                    if (Convert.ToInt32(evt["et"]) == 4)
                        Interlocked.Add(ref _logEntryCount, ((ICollection)evt["e"]).Count);
                }
            }

            public void Dispose()
            {
                _cts.Cancel();
                _listener.Close();
                _cts.Dispose();
            }
        }
    }
}
