using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Reflection;
using System.Threading.Tasks;
using NUnit.Framework;
using TestRift.NUnit;

namespace TestRift.NUnit.Tests
{
    [TestFixture]
    [NonParallelizable]
    public sealed class KpiUploadTests
    {
        private string _kpiFilePath;
        private byte[] _previousKpiFile;
        private bool _previousKpiFileExisted;

        [SetUp]
        public void SetUp()
        {
            _kpiFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "kpi.json");
            _previousKpiFileExisted = File.Exists(_kpiFilePath);
            _previousKpiFile = _previousKpiFileExisted ? File.ReadAllBytes(_kpiFilePath) : null;
            if (_previousKpiFileExisted)
                File.Delete(_kpiFilePath);
        }

        [TearDown]
        public void TearDown()
        {
            if (_previousKpiFileExisted)
                File.WriteAllBytes(_kpiFilePath, _previousKpiFile);
            else if (File.Exists(_kpiFilePath))
                File.Delete(_kpiFilePath);
        }

        [Test]
        public async Task WebSocketHelper_SkipsKpiFileUnchangedSinceRunStart()
        {
            File.WriteAllText(_kpiFilePath, "{\"_schemaVersion\":1,\"_samples\":[]}");
            using (var listener = CreateListener(out var serverUrl))
            using (var helper = new WebSocketHelper(serverUrl))
            {
                PrepareRun(helper, "stale-run");
                var requestTask = listener.GetContextAsync();

                await UploadKpiFileAsync(helper);

                var completed = await Task.WhenAny(requestTask, Task.Delay(300));
                if (completed == requestTask)
                {
                    var context = await requestTask;
                    context.Response.StatusCode = (int)HttpStatusCode.NoContent;
                    context.Response.Close();
                }

                Assert.That(completed, Is.Not.SameAs(requestTask), "Unchanged KPI data must not be uploaded again.");
                listener.Stop();
                try
                {
                    await requestTask;
                }
                catch (HttpListenerException)
                {
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }

        [Test]
        public async Task WebSocketHelper_UploadsChangedKpiFileToCurrentRun()
        {
            File.WriteAllText(_kpiFilePath, "{\"_schemaVersion\":1,\"_samples\":[]}");
            using (var listener = CreateListener(out var serverUrl))
            using (var helper = new WebSocketHelper(serverUrl))
            {
                PrepareRun(helper, "fresh-run");
                const string newContent = "{\"_schemaVersion\":1,\"_samples\":[{\"metric_key\":\"measurement.value\",\"unit\":\"ms\",\"value\":2,\"test_name\":\"Example.Test\"}]}";
                File.WriteAllText(_kpiFilePath, newContent);

                var requestTask = listener.GetContextAsync();
                var uploadTask = UploadKpiFileAsync(helper);
                var context = await requestTask.WaitAsync(TimeSpan.FromSeconds(5));
                string requestBody;
                using (var reader = new StreamReader(context.Request.InputStream, context.Request.ContentEncoding))
                    requestBody = await reader.ReadToEndAsync();

                context.Response.StatusCode = (int)HttpStatusCode.NoContent;
                context.Response.Close();
                await uploadTask;

                Assert.That(context.Request.HttpMethod, Is.EqualTo("POST"));
                Assert.That(context.Request.Url.AbsolutePath, Is.EqualTo("/api/runs/fresh-run/kpis"));
                Assert.That(requestBody, Is.EqualTo(newContent));
            }
        }

        [Test]
        public async Task WebSocketHelper_UploadsFreshKpiFileWithIdenticalContent()
        {
            const string content = "{\"_schemaVersion\":1,\"_samples\":[]}";
            File.WriteAllText(_kpiFilePath, content);
            var previousWriteTime = DateTime.UtcNow.AddMinutes(-1);
            File.SetLastWriteTimeUtc(_kpiFilePath, previousWriteTime);
            using (var listener = CreateListener(out var serverUrl))
            using (var helper = new WebSocketHelper(serverUrl))
            {
                PrepareRun(helper, "fresh-identical-run");
                File.WriteAllText(_kpiFilePath, content);
                File.SetLastWriteTimeUtc(_kpiFilePath, previousWriteTime.AddMinutes(1));
                var requestTask = listener.GetContextAsync();
                var uploadTask = UploadKpiFileAsync(helper);
                var context = await requestTask.WaitAsync(TimeSpan.FromSeconds(5));
                context.Response.StatusCode = (int)HttpStatusCode.NoContent;
                context.Response.Close();
                await uploadTask;
                Assert.That(helper.KpiUploadFailure, Is.Null);
            }
        }

        [Test]
        public async Task WebSocketHelper_ClearsFailureWhenRetrySucceeds()
        {
            using (var listener = CreateListener(out var serverUrl))
            using (var helper = new WebSocketHelper(serverUrl))
            {
                PrepareRun(helper, "retry-run");
                File.WriteAllText(_kpiFilePath, "{\"_schemaVersion\":1,\"_samples\":[]}");
                var firstRequest = listener.GetContextAsync();
                var uploadTask = UploadKpiFileAsync(helper);
                var first = await firstRequest.WaitAsync(TimeSpan.FromSeconds(5));
                first.Response.StatusCode = (int)HttpStatusCode.ServiceUnavailable;
                first.Response.Close();
                var second = await listener.GetContextAsync().WaitAsync(TimeSpan.FromSeconds(5));
                Assert.That(helper.KpiUploadFailure, Is.Not.Null);
                second.Response.StatusCode = (int)HttpStatusCode.NoContent;
                second.Response.Close();
                await uploadTask;
                Assert.That(helper.KpiUploadFailure, Is.Null);
            }
        }

        [TestCase(1024, 1)]
        [TestCase(64 * 1024 + 1, 60)]
        public async Task WebSocketHelper_IncompleteErrorBodiesDoNotBlockTeardown(int responseLength, int timeoutSeconds)
        {
            using (var listener = CreateListener(out var serverUrl))
            using (var helper = new WebSocketHelper(serverUrl))
            {
                PrepareRun(helper, "stalled-run");
                File.WriteAllText(_kpiFilePath, "{\"_schemaVersion\":1,\"_samples\":[]}");
                var httpClient = (HttpClient)typeof(WebSocketHelper)
                    .GetField("_httpClient", BindingFlags.Instance | BindingFlags.NonPublic)
                    .GetValue(helper);
                httpClient.Timeout = TimeSpan.FromSeconds(timeoutSeconds);
                var uploadTask = UploadKpiFileAsync(helper);
                var stalledResponses = new System.Collections.Generic.List<HttpListenerResponse>();
                try
                {
                    for (var attempt = 0; attempt < 2; attempt++)
                    {
                        var context = await listener.GetContextAsync().WaitAsync(TimeSpan.FromSeconds(5));
                        stalledResponses.Add(context.Response);
                        context.Response.StatusCode = (int)HttpStatusCode.ServiceUnavailable;
                        context.Response.ContentLength64 = responseLength;
                        await context.Response.OutputStream.WriteAsync(new byte[] { 65 }, 0, 1);
                        await context.Response.OutputStream.FlushAsync();
                    }
                    await uploadTask.WaitAsync(TimeSpan.FromSeconds(5));
                    Assert.That(helper.KpiUploadFailure, Is.Not.Null);
                }
                finally
                {
                    foreach (var response in stalledResponses)
                        response.Close();
                }
            }
        }

        [Test]
        public async Task WebSocketHelper_SendsEnvironmentIngestTokenWithKpis()
        {
            const string token = "test-ingest-token";
            var configField = typeof(ConfigManager).GetField("_config", BindingFlags.Static | BindingFlags.NonPublic);
            var previousConfig = configField.GetValue(null);
            var previousToken = Environment.GetEnvironmentVariable("TESTRIFT_INGEST_TOKEN");
            var configPath = Path.GetTempFileName();
            try
            {
                Environment.SetEnvironmentVariable("TESTRIFT_INGEST_TOKEN", token);
                File.WriteAllText(configPath, string.Join("\n",
                    "serverUrl: http://127.0.0.1:1",
                    "ingestToken: yaml-token",
                    "target: test-target",
                    "purpose: manual",
                    "sources:",
                    "  test-system:",
                    "    branch: main",
                    "    revision: test-revision"));
                ConfigManager.Load(configPath);
                using (var listener = CreateListener(out var serverUrl))
                using (var helper = new WebSocketHelper(serverUrl))
                {
                    PrepareRun(helper, "authenticated-run");
                    File.WriteAllText(_kpiFilePath, "{\"_schemaVersion\":1,\"_samples\":[]}");
                    var requestTask = listener.GetContextAsync();
                    var uploadTask = UploadKpiFileAsync(helper);
                    var context = await requestTask.WaitAsync(TimeSpan.FromSeconds(5));
                    context.Response.StatusCode = (int)HttpStatusCode.NoContent;
                    context.Response.Close();
                    await uploadTask;
                    Assert.That(context.Request.Headers["X-TestRift-Ingest-Token"], Is.EqualTo(token));
                }
            }
            finally
            {
                Environment.SetEnvironmentVariable("TESTRIFT_INGEST_TOKEN", previousToken);
                configField.SetValue(null, previousConfig);
                File.Delete(configPath);
            }
        }

        private static HttpListener CreateListener(out string serverUrl)
        {
            int port;
            var reservation = new TcpListener(IPAddress.Loopback, 0);
            try
            {
                reservation.Start();
                port = ((IPEndPoint)reservation.LocalEndpoint).Port;
            }
            finally
            {
                reservation.Stop();
            }

            serverUrl = $"http://127.0.0.1:{port}";
            var listener = new HttpListener();
            listener.Prefixes.Add(serverUrl + "/");
            listener.Start();
            return listener;
        }

        private static void PrepareRun(WebSocketHelper helper, string runId)
        {
            var helperType = typeof(WebSocketHelper);
            helperType.GetField("_runId", BindingFlags.Instance | BindingFlags.NonPublic)
                .SetValue(helper, runId);
            helperType.GetMethod("CaptureKpiFileState", BindingFlags.Instance | BindingFlags.NonPublic)
                .Invoke(helper, null);
        }

        private static Task UploadKpiFileAsync(WebSocketHelper helper)
        {
            return (Task)typeof(WebSocketHelper)
                .GetMethod("UploadKpiFileAsync", BindingFlags.Instance | BindingFlags.NonPublic)
                .Invoke(helper, null);
        }
    }
}