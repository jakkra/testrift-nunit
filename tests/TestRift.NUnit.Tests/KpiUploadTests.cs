using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Threading.Tasks;
using NUnit.Framework;
using TestRift.NUnit;

namespace TestRift.NUnit.Tests
{
    [TestFixture]
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
            File.WriteAllText(_kpiFilePath, "{\"samples\":[]}");
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
            File.WriteAllText(_kpiFilePath, "{\"samples\":[{\"value\":1}]}");
            using (var listener = CreateListener(out var serverUrl))
            using (var helper = new WebSocketHelper(serverUrl))
            {
                PrepareRun(helper, "fresh-run");
                const string newContent = "{\"samples\":[{\"value\":2}]}";
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