using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MessagePack;

namespace TestRift.NUnit
{
    /// <summary>
    /// Protocol constants for optimized MessagePack wire format.
    /// See docs/websocket_protocol.md for complete specification.
    /// </summary>
    internal static class Protocol
    {
        // Message types (t field)
        public const int MSG_RUN_STARTED = 1;
        public const int MSG_RUN_STARTED_RESPONSE = 2;
        public const int MSG_TEST_CASE_STARTED = 3;
        public const int MSG_LOG_BATCH = 4;
        public const int MSG_EXCEPTION = 5;
        public const int MSG_TEST_CASE_FINISHED = 6;
        public const int MSG_RUN_FINISHED = 7;
        public const int MSG_BATCH = 8;
        public const int MSG_HEARTBEAT = 9;
        public const int MSG_METRICS = 11;

        // Status codes (s field)
        public const int STATUS_RUNNING = 1;
        public const int STATUS_PASSED = 2;
        public const int STATUS_FAILED = 3;
        public const int STATUS_SKIPPED = 4;
        public const int STATUS_ABORTED = 5;
        public const int STATUS_FINISHED = 6;

        // Direction codes (d field)
        public const int DIR_TX = 1;
        public const int DIR_RX = 2;

        // Phase codes (p field)
        public const int PHASE_TEARDOWN = 1;

        // Field keys (short names for MessagePack maps)
        public const string F_TYPE = "t";
        public const string F_RUN_ID = "r";
        public const string F_RUN_NAME = "n";
        public const string F_STATUS = "s";
        public const string F_TIMESTAMP = "ts";
        public const string F_TC_FULL_NAME = "f";
        public const string F_TC_ID = "i";
        public const string F_MESSAGE = "m";
        public const string F_COMPONENT = "c";
        public const string F_CHANNEL = "ch";
        public const string F_DIR = "d";
        public const string F_PHASE = "p";
        public const string F_ENTRIES = "e";
        public const string F_EVENTS = "ev";
        public const string F_EVENT_TYPE = "et";
        public const string F_EXCEPTION_TYPE = "xt";
        public const string F_STACK_TRACE = "st";
        public const string F_IS_ERROR = "ie";
        public const string F_USER_METADATA = "md";
        public const string F_RETENTION_DAYS = "rd";
        public const string F_LOCAL_RUN = "lr";
        public const string F_ERROR = "err";
        public const string F_RUN_URL = "ru";
        public const string F_TARGET_KEY = "tk";
        public const string F_PURPOSE = "pu";
        public const string F_PARENT_RUN_ID = "pr";
        public const string F_SOURCES = "so";
        public const string F_SOURCE_BRANCH = "br";
        public const string F_SOURCE_REVISION = "rv";
        public const string F_SOURCE_REPOSITORY_URL = "repo";
        public const string F_SOURCE_DIRTY = "dy";
        public const string F_TARGET_URL = "tu";

        // Metrics fields
        public const string F_METRICS = "mt";
        public const string F_CPU = "cpu";
        public const string F_MEMORY = "mem";
        public const string F_NET = "net";
        public const string F_NET_INTERFACES = "ni";

        /// <summary>Unix epoch for timestamp calculations.</summary>
        private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>Convert DateTime to milliseconds since Unix epoch.</summary>
        public static long ToMs(DateTime dt) => (long)(dt.ToUniversalTime() - UnixEpoch).TotalMilliseconds;

        /// <summary>Get current timestamp in milliseconds since Unix epoch.</summary>
        public static long NowMs() => ToMs(DateTime.UtcNow);

        /// <summary>Convert status string to status code.</summary>
        public static int StatusToCode(string status)
        {
            switch (status?.ToLower())
            {
                case "running": return STATUS_RUNNING;
                case "passed": return STATUS_PASSED;
                case "failed": return STATUS_FAILED;
                case "skipped": return STATUS_SKIPPED;
                case "aborted": return STATUS_ABORTED;
                case "finished": return STATUS_FINISHED;
                case "error": return STATUS_FAILED;  // Map error to failed
                default: return STATUS_FAILED;
            }
        }

        /// <summary>Convert direction string to direction code.</summary>
        public static int? DirToCode(string dir)
        {
            switch (dir?.ToLower())
            {
                case "tx": return DIR_TX;
                case "rx": return DIR_RX;
                default: return null;
            }
        }

        /// <summary>Convert phase string to phase code.</summary>
        public static int? PhaseToCode(string phase)
        {
            switch (phase?.ToLower())
            {
                case "teardown": return PHASE_TEARDOWN;
                default: return null;
            }
        }
    }

    /// <summary>
    /// Helper class for managing WebSocket connections and high-performance event batching.
    /// Uses a unified event queue to batch test case starts, finishes, logs, and exceptions
    /// into single WebSocket frames for optimal throughput.
    /// </summary>
    public class WebSocketHelper : IDisposable
    {
        private const int BatchIntervalMs = 250;        // Send batches every 250ms
        private const int MaxBatchSizeBytes = 131072;   // Flush when buffered data exceeds 128KB
        private const int MaxEventsPerBatch = 500;      // Maximum events per batch message
        private const int HeartbeatIntervalMs = 10000;  // Send heartbeat every 10 seconds if no activity
        private const long MaxKpiUploadBytes = 10 * 1024 * 1024;
        private static readonly TimeSpan ConnectTimeout = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan SendTimeout = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan CloseTimeout = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan HttpUploadTimeout = TimeSpan.FromSeconds(60);

        private long _droppedEventCount;
        private volatile string _kpiUploadFailure;

        /// <summary>Events that could not be delivered because sending failed or the connection was lost.</summary>
        public long DroppedEventCount => Interlocked.Read(ref _droppedEventCount);

        /// <summary>Reason the KPI upload failed, or null when it succeeded or was not needed.</summary>
        public string KpiUploadFailure => _kpiUploadFailure;

        /// <summary>True when the server accepted run_started and issued a run ID.</summary>
        public bool IsRunActive => !string.IsNullOrEmpty(_runId);

        private readonly string _serverBaseUrl;
        private string _runId;  // Set by server response after run_started
        private string _kpiFileFingerprintAtRunStart;
        private bool _kpiFileStateCaptured;
        private ClientWebSocket _webSocket;

        // String interning table for component/channel names
        private readonly ConcurrentDictionary<string, int> _stringTable = new ConcurrentDictionary<string, int>();
        private int _nextStringId = 1;

        // Unified event queue for all event types (TC start, TC finish, logs, exceptions)
        private readonly ConcurrentQueue<BatchEvent> _eventQueue = new ConcurrentQueue<BatchEvent>();

        /// <summary>
        /// Get or register an interned string. Returns [id, value] for first use, just id for subsequent.
        /// </summary>
        private object GetInternedString(string value)
        {
            if (string.IsNullOrEmpty(value)) return null;

            if (_stringTable.TryGetValue(value, out var existingId))
            {
                return existingId;  // Already registered, return just the ID
            }

            // Register new string with next available ID
            var newId = Interlocked.Increment(ref _nextStringId);
            if (_stringTable.TryAdd(value, newId))
            {
                // First occurrence: return [id, value]
                return new object[] { newId, value };
            }
            else
            {
                // Race condition: another thread registered it
                return _stringTable[value];
            }
        }
        private readonly Timer _batchTimer;
        private readonly Timer _heartbeatTimer;
        private readonly Timer _metricsTimer;
        private readonly SystemMetricsCollector _metricsCollector;
        private bool _disposed = false;
        private readonly SemaphoreSlim _sendSemaphore = new SemaphoreSlim(1, 1);
        private readonly HttpClient _httpClient;
        private readonly string _ingestToken;
        private CancellationTokenSource _receiveLoopCts;
        private Task _receiveLoopTask;
        private long _messageSequence;
        private DateTime _lastSendTime = DateTime.UtcNow;

        // Tracks test cases that have reported unexpected/unhandled errors (is_error = true)
        private readonly ConcurrentDictionary<string, bool> _testCasesWithErrors = new ConcurrentDictionary<string, bool>();
        // Maps tc_full_name to tc_id
        private readonly ConcurrentDictionary<string, string> _testCaseIds = new ConcurrentDictionary<string, string>();
        // Maps NUnit test.Id to tc_id (for concurrent test execution)
        private readonly ConcurrentDictionary<string, string> _testIdToTcId = new ConcurrentDictionary<string, string>();

        // Estimated current batch size for triggering early flush
        private long _estimatedBatchSize = 0;

        // MessagePack serializer options - use ContractlessStandardResolver for dictionary serialization
        private static readonly MessagePackSerializerOptions _msgpackOptions = MessagePackSerializerOptions.Standard
            .WithResolver(MessagePack.Resolvers.ContractlessStandardResolver.Instance);

        public WebSocketHelper(string serverBaseUrl = null)
        {
            if (!string.IsNullOrWhiteSpace(serverBaseUrl))
            {
                _serverBaseUrl = serverBaseUrl;
            }
            else
            {
                try
                {
                    var cfg = ConfigManager.Get();
                    _serverBaseUrl = !string.IsNullOrWhiteSpace(cfg.ServerUrl) ? cfg.ServerUrl : "http://localhost:8080";
                }
                catch
                {
                    _serverBaseUrl = "http://localhost:8080";
                }
            }
            _runId = null;

            // Initialize batch timer for regular flushing
            _batchTimer = new Timer(OnBatchTimerTick, null, BatchIntervalMs, BatchIntervalMs);

            // Initialize heartbeat timer to keep connection alive during idle periods
            _heartbeatTimer = new Timer(OnHeartbeatTimerTick, null, HeartbeatIntervalMs, HeartbeatIntervalMs);

            // Initialize metrics collector (starts when run starts)
            _metricsCollector = new SystemMetricsCollector(1000);

            // Initialize metrics timer (sends every 5 seconds to batch samples)
            _metricsTimer = new Timer(OnMetricsTimerTick, null, Timeout.Infinite, Timeout.Infinite);

            _httpClient = new HttpClient
            {
                Timeout = HttpUploadTimeout,
                MaxResponseContentBufferSize = 64 * 1024
            };
            _ingestToken = GetConfiguredIngestToken();
            if (!string.IsNullOrWhiteSpace(_ingestToken))
                _httpClient.DefaultRequestHeaders.TryAddWithoutValidation("x-testrift-ingest-token", _ingestToken);
        }

        private static string GetConfiguredIngestToken()
        {
            try
            {
                return ConfigManager.Get().IngestToken;
            }
            catch
            {
                return null;
            }
        }

        private void CaptureKpiFileState()
        {
            try
            {
                var filePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "kpi.json");
                _kpiFileFingerprintAtRunStart = GetKpiFileFingerprint(filePath);
                _kpiFileStateCaptured = true;
            }
            catch (Exception ex)
            {
                _kpiFileStateCaptured = false;
                ThreadSafeFileLogger.LogWebSocketConnectionFailed(
                    $"KPI file freshness check could not start: {ex.Message}"
                );
            }
        }

        private static string GetKpiFileFingerprint(string filePath)
        {
            if (!File.Exists(filePath))
                return null;

            var fileInfo = new FileInfo(filePath);
            if (fileInfo.Length > MaxKpiUploadBytes)
                return null;

            using (var stream = File.OpenRead(filePath))
            using (var sha256 = SHA256.Create())
            {
                return $"{fileInfo.LastWriteTimeUtc.Ticks}:{Convert.ToBase64String(sha256.ComputeHash(stream))}";
            }
        }

        public async Task ConnectAsync()
        {
            try
            {
                _webSocket = new ClientWebSocket();
                _webSocket.Options.KeepAliveInterval = TimeSpan.FromSeconds(30);
                if (!string.IsNullOrWhiteSpace(_ingestToken))
                    _webSocket.Options.SetRequestHeader("x-testrift-ingest-token", _ingestToken);
                var uri = new Uri(_serverBaseUrl.Replace("http://", "ws://").Replace("https://", "wss://") + "/ws/nunit");
                using (var timeout = new CancellationTokenSource(ConnectTimeout))
                {
                    await _webSocket.ConnectAsync(uri, timeout.Token);
                }

                ThreadSafeFileLogger.LogWebSocketConnected(uri.ToString());
                ThreadSafeFileLogger.LogWebSocketStateDetails("Post-connect", _webSocket.State, _webSocket.CloseStatus, _webSocket.CloseStatusDescription);
            }
            catch (Exception ex)
            {
                ThreadSafeFileLogger.LogWebSocketException("ConnectAsync", ex);
                ThreadSafeFileLogger.LogWebSocketConnectionFailed(ex.Message);
            }
        }

        public async Task SendRunStarted()
        {
            CaptureKpiFileState();

            // Check if we're activating a prepared run (from testrift-collector)
            var preparedRunId = GetPreparedRunId();

            var runId = GetRunId();
            var runName = GetRunName();
            var timestamp = Protocol.NowMs();
            var userMetadata = GetUserMetadata();
            var dataDict = new Dictionary<string, object>
            {
                { Protocol.F_TYPE, Protocol.MSG_RUN_STARTED },
                { Protocol.F_RUN_NAME, runName },
                { Protocol.F_TIMESTAMP, timestamp },
                { Protocol.F_USER_METADATA, userMetadata }
            };

            // If we have a prepared run ID, use it to activate the prepared run
            if (!string.IsNullOrEmpty(preparedRunId))
            {
                dataDict[Protocol.F_RUN_ID] = preparedRunId;
                ThreadSafeFileLogger.LogMessageSent($"Activating prepared run: {preparedRunId}");
            }
            else if (!string.IsNullOrEmpty(runId))
            {
                dataDict[Protocol.F_RUN_ID] = runId;
            }

            if (string.IsNullOrEmpty(preparedRunId))
            {
                var context = GetRunContext();
                dataDict[Protocol.F_TARGET_KEY] = context.Target;
                dataDict[Protocol.F_PURPOSE] = context.Purpose;
                dataDict[Protocol.F_SOURCES] = context.Sources;
                if (!string.IsNullOrEmpty(context.ParentRunId))
                    dataDict[Protocol.F_PARENT_RUN_ID] = context.ParentRunId;
            }

            await SendWebSocketMessage(dataDict);
            await WaitForRunStartedResponse();
            StartPassiveReceiveLoop();

            // Start metrics collection after successful connection
            StartMetricsCollection();
        }

        private string GetPreparedRunId()
        {
            // Read from environment variable only (not from config file)
            return Environment.GetEnvironmentVariable("TESTRIFT_PREPARED_RUN_ID");
        }

        private string GetRunName()
        {
            try
            {
                var config = ConfigManager.Get();
                return config.RunName;
            }
            catch
            {
                return null;
            }
        }

        private string GetRunId()
        {
            try
            {
                var config = ConfigManager.Get();
                return config.RunId;
            }
            catch
            {
                return null;
            }
        }

        private async Task WaitForRunStartedResponse()
        {
            try
            {
                if (_webSocket?.State != WebSocketState.Open)
                {
                    var stateSnapshot = DescribeSocketState();
                    ThreadSafeFileLogger.LogWebSocketNotOpen(stateSnapshot);
                    throw new InvalidOperationException($"WebSocket connection is not open (state: {stateSnapshot})");
                }

                var buffer = new byte[4096];
                var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

                var result = await _webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), cts.Token);

                if (result.MessageType == WebSocketMessageType.Binary)
                {
                    var data = new ReadOnlyMemory<byte>(buffer, 0, result.Count);
                    var response = MessagePackSerializer.Deserialize<Dictionary<string, object>>(data, _msgpackOptions);

                    // Check for message type (numeric code)
                    // MessagePack deserializes small integers as byte, not int
                    if (response.TryGetValue(Protocol.F_TYPE, out var typeObj) &&
                        TryGetInt(typeObj, out var typeCode) &&
                        typeCode == Protocol.MSG_RUN_STARTED_RESPONSE)
                    {
                        // Check for error
                        if (response.TryGetValue(Protocol.F_ERROR, out var errorObj) && errorObj != null)
                        {
                            var errorMessage = errorObj.ToString();
                            ThreadSafeFileLogger.LogWebSocketConnectionFailed($"Run start failed: {errorMessage}");
                            throw new InvalidOperationException($"Run start failed: {errorMessage}");
                        }

                        // Get run_id
                        if (response.TryGetValue(Protocol.F_RUN_ID, out var runIdObj) && runIdObj != null)
                        {
                            _runId = runIdObj.ToString();
                            ThreadSafeFileLogger.LogMessageSent($"Received run_id from server: {_runId}");
                        }

                        WriteUrlFilesFromResponse(response);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                ThreadSafeFileLogger.LogWebSocketConnectionFailed("Timeout waiting for run_started_response");
                throw new InvalidOperationException("Timeout waiting for run_started_response from server");
            }
        }

        private void StartPassiveReceiveLoop()
        {
            if (_receiveLoopTask != null && !_receiveLoopTask.IsCompleted)
                return;

            _receiveLoopCts?.Cancel();
            _receiveLoopCts?.Dispose();
            _receiveLoopCts = new CancellationTokenSource();

            ThreadSafeFileLogger.Log("Passive receive loop starting");
            _receiveLoopTask = Task.Run(() => PassiveReceiveLoopAsync(_receiveLoopCts.Token));
        }

        private async Task PassiveReceiveLoopAsync(CancellationToken token)
        {
            var buffer = new byte[4096];

            while (!token.IsCancellationRequested)
            {
                if (_webSocket == null)
                {
                    await Task.Delay(250, token);
                    continue;
                }

                try
                {
                    var result = await _webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), token);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        ThreadSafeFileLogger.LogWebSocketConnectionFailed($"Server closed WebSocket: {result.CloseStatus} {result.CloseStatusDescription}");
                        using (var closeTimeout = new CancellationTokenSource(CloseTimeout))
                        {
                            await _webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closing", closeTimeout.Token);
                        }
                        break;
                    }

                    if (result.MessageType == WebSocketMessageType.Binary && result.Count > 0)
                    {
                        ThreadSafeFileLogger.Log($"Received server message ({result.Count} bytes)");
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ObjectDisposedException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    ThreadSafeFileLogger.LogWebSocketException("Passive receive loop", ex);
                    await Task.Delay(1000, token);
                }
            }
        }

        private void StopPassiveReceiveLoop()
        {
            if (_receiveLoopCts == null) return;

            try
            {
                ThreadSafeFileLogger.Log("Stopping passive receive loop");
                _receiveLoopCts.Cancel();
                _receiveLoopTask?.Wait(TimeSpan.FromSeconds(5));
            }
            catch (AggregateException ex)
            {
                ThreadSafeFileLogger.LogWebSocketConnectionFailed($"Error stopping receive loop: {ex.InnerException?.Message ?? ex.Message}");
            }
            finally
            {
                ThreadSafeFileLogger.Log("Passive receive loop stopped");
                _receiveLoopCts.Dispose();
                _receiveLoopCts = null;
                _receiveLoopTask = null;
            }
        }

        private void WriteUrlFilesFromResponse(Dictionary<string, object> response)
        {
            try
            {
                var config = ConfigManager.Get();
                if (config.UrlFiles == null) return;

                var runUrlFile = config.UrlFiles.RunUrlFile;
                var targetUrlFile = config.UrlFiles.TargetUrlFile;

                if (!string.IsNullOrEmpty(runUrlFile) &&
                    response.TryGetValue(Protocol.F_RUN_URL, out var runUrlObj) &&
                    runUrlObj != null)
                {
                    var runUrl = _serverBaseUrl + runUrlObj.ToString();
                    WriteUrlFile(runUrlFile, runUrl);
                }

                if (!string.IsNullOrEmpty(targetUrlFile) &&
                    response.TryGetValue(Protocol.F_TARGET_URL, out var targetUrlObj) &&
                    targetUrlObj != null)
                {
                    var targetUrl = _serverBaseUrl + targetUrlObj.ToString();
                    WriteUrlFile(targetUrlFile, targetUrl);
                }
            }
            catch (Exception ex)
            {
                ThreadSafeFileLogger.LogWebSocketConnectionFailed($"Error writing URL files from response: {ex.Message}");
            }
        }

        private Dictionary<string, object> GetUserMetadata()
        {
            try
            {
                var config = ConfigManager.Get();
                return BuildMetadataDictionary(config.Metadata);
            }
            catch (Exception ex)
            {
                ThreadSafeFileLogger.LogWebSocketConnectionFailed($"Failed to get user metadata: {ex.Message}");
            }
            return new Dictionary<string, object>();
        }

        private (string Target, string Purpose, string ParentRunId, Dictionary<string, object> Sources) GetRunContext()
        {
            var config = ConfigManager.Get();
            var sources = new Dictionary<string, object>();
            foreach (var source in config.Sources)
            {
                var value = source.Value;
                var wireSource = new Dictionary<string, object>
                {
                    { Protocol.F_SOURCE_BRANCH, value.Branch },
                    { Protocol.F_SOURCE_REVISION, value.Revision },
                };
                if (!string.IsNullOrEmpty(value.RepositoryUrl))
                    wireSource[Protocol.F_SOURCE_REPOSITORY_URL] = value.RepositoryUrl;
                if (value.Dirty.HasValue)
                    wireSource[Protocol.F_SOURCE_DIRTY] = value.Dirty.Value;
                sources[source.Key] = wireSource;
            }
            return (config.Target, config.Purpose, config.ParentRunId, sources);
        }

        private static Dictionary<string, object> BuildMetadataDictionary(IEnumerable<MetadataEntry> entries)
        {
            var metadata = new Dictionary<string, object>();
            if (entries == null) return metadata;

            foreach (var entry in entries)
            {
                if (entry == null) continue;

                var expandedName = VarExpander.Expand(entry.Name ?? string.Empty).Trim();
                if (string.IsNullOrWhiteSpace(expandedName)) continue;

                var expandedValue = VarExpander.Expand(entry.Value ?? string.Empty);
                var expandedUrl = entry.Url != null ? VarExpander.Expand(entry.Url) : null;

                metadata[expandedName] = new
                {
                    value = expandedValue,
                    url = expandedUrl
                };
            }
            return metadata;
        }

        public async Task SendRunFinished()
        {
            // Stop metrics collection and send final batch
            StopMetricsCollection();
            await FlushMetricsAsync();

            await FlushAllMessagesAsync();

            await UploadKpiFileAsync();

            var data = new Dictionary<string, object>
            {
                { Protocol.F_TYPE, Protocol.MSG_RUN_FINISHED },
                { Protocol.F_RUN_ID, _runId },
                { Protocol.F_STATUS, Protocol.STATUS_FINISHED },
                { Protocol.F_TIMESTAMP, Protocol.NowMs() }
            };

            await SendWebSocketMessage(data);

            // Gracefully close the WebSocket after run_finished to ensure server receives the message
            await CloseAsync();
        }

        private async Task UploadKpiFileAsync()
        {
            if (string.IsNullOrWhiteSpace(_runId)) return;

            var filePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "kpi.json");
            if (!File.Exists(filePath))
            {
                ThreadSafeFileLogger.LogMessageSent("KPI upload skipped: kpi.json was not found");
                return;
            }

            FileInfo fileInfo;
            try
            {
                fileInfo = new FileInfo(filePath);
                if (fileInfo.Length == 0)
                {
                    ThreadSafeFileLogger.LogMessageSent("KPI upload skipped: kpi.json is empty");
                    return;
                }
                if (fileInfo.Length > MaxKpiUploadBytes)
                {
                    RecordKpiUploadFailure($"kpi.json exceeds {MaxKpiUploadBytes} bytes");
                    return;
                }

                if (!_kpiFileStateCaptured)
                {
                    RecordKpiUploadFailure("file freshness could not be verified");
                    return;
                }

                var currentFingerprint = GetKpiFileFingerprint(filePath);
                if (string.Equals(
                    currentFingerprint,
                    _kpiFileFingerprintAtRunStart,
                    StringComparison.Ordinal))
                {
                    ThreadSafeFileLogger.LogMessageSent(
                        "KPI upload skipped: kpi.json is unchanged since run start"
                    );
                    return;
                }
            }
            catch (Exception ex)
            {
                RecordKpiUploadFailure($"inspection failed: {ex.Message}");
                return;
            }

            var uploadUrl = $"{_serverBaseUrl.TrimEnd('/')}/api/runs/{Uri.EscapeDataString(_runId)}/kpis";
            for (var attempt = 1; attempt <= 2; attempt++)
            {
                try
                {
                    using (var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
                    using (var request = new HttpRequestMessage(HttpMethod.Post, uploadUrl))
                    using (var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read))
                    using (var content = new StreamContent(stream))
                    {
                        content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");
                        request.Content = content;

                        using (var response = await _httpClient.SendAsync(
                            request,
                            HttpCompletionOption.ResponseContentRead,
                            timeout.Token))
                        {
                            if (response.IsSuccessStatusCode)
                            {
                                _kpiUploadFailure = null;
                                ThreadSafeFileLogger.LogMessageSent($"Uploaded KPI file for run {_runId}");
                                return;
                            }

                            var statusCode = (int)response.StatusCode;
                            var transient = statusCode == 408 || statusCode == 429 || statusCode >= 500;
                            var detail = await ReadErrorDetailAsync(response);
                            RecordKpiUploadFailure($"HTTP {statusCode}{detail} (attempt {attempt}/2)");
                            if (!transient) return;
                        }
                    }
                }
                catch (Exception ex)
                {
                    RecordKpiUploadFailure($"{ex.Message} (attempt {attempt}/2)");
                }

                if (attempt < 2)
                    await Task.Delay(250);
            }
        }

        /// <summary>
        /// Queue a test case started event for batched sending.
        /// </summary>
        public void QueueTestCaseStarted(string tcFullName, string nunitTestId, string startTime = null)
        {
            var tcId = nunitTestId;

            _testCaseIds[tcFullName] = tcId;
            if (!string.IsNullOrEmpty(nunitTestId))
            {
                _testIdToTcId[nunitTestId] = tcId;
            }

            // Parse timestamp if provided as ISO string, otherwise use current time
            long timestamp;
            if (!string.IsNullOrEmpty(startTime) && DateTime.TryParse(startTime, out var dt))
                timestamp = Protocol.ToMs(dt);
            else
                timestamp = Protocol.NowMs();

            var eventData = new Dictionary<string, object>
            {
                { Protocol.F_EVENT_TYPE, Protocol.MSG_TEST_CASE_STARTED },
                { Protocol.F_TC_FULL_NAME, tcFullName },
                { Protocol.F_TC_ID, tcId },
                { Protocol.F_STATUS, Protocol.STATUS_RUNNING },
                { Protocol.F_TIMESTAMP, timestamp }
            };

            QueueEvent(new BatchEvent { EventType = Protocol.MSG_TEST_CASE_STARTED, Data = eventData, TestCaseId = tcId });
        }

        /// <summary>
        /// Queue a test case finished event for batched sending.
        /// Automatically flushes pending events for this test case first.
        /// </summary>
        public async Task QueueTestCaseFinishedAsync(string nunitTestId, string result)
        {
            // Flush all pending events for this test case to ensure ordering
            await FlushEventsForTestCaseAsync(nunitTestId);

            int statusCode = ConvertNUnitResultAndErrorFlagToStatusCode(result, nunitTestId);
            string tcId = nunitTestId;

            if (string.IsNullOrEmpty(tcId))
            {
                ThreadSafeFileLogger.LogWebSocketConnectionFailed($"tc_id (NUnit test ID) is empty for test case");
                return;
            }

            var eventData = new Dictionary<string, object>
            {
                { Protocol.F_EVENT_TYPE, Protocol.MSG_TEST_CASE_FINISHED },
                { Protocol.F_TC_ID, tcId },
                { Protocol.F_STATUS, statusCode },
                { Protocol.F_TIMESTAMP, Protocol.NowMs() }
            };

            // For test case finished, send immediately (after flushing its pending events)
            QueueEvent(new BatchEvent { EventType = Protocol.MSG_TEST_CASE_FINISHED, Data = eventData, TestCaseId = tcId });
            await FlushBatchAsync();
        }

        /// <summary>
        /// Async method for test case started - queues and flushes for immediate sending.
        /// </summary>
        public async Task SendTestCaseStarted(string tcFullName, string nunitTestId, string startTime = null)
        {
            QueueTestCaseStarted(tcFullName, nunitTestId, startTime);
            // For TC start, we want it sent quickly so trigger a flush
            await FlushBatchAsync();
        }

        /// <summary>
        /// Async method for test case finished.
        /// </summary>
        public async Task SendTestCaseFinished(string nunitTestId, string result)
        {
            await QueueTestCaseFinishedAsync(nunitTestId, result);
        }

        public string GetTcIdFromTestId(string nunitTestId)
        {
            return _testIdToTcId.TryGetValue(nunitTestId, out var tcId) ? tcId : null;
        }

        public string GetTcIdFromFullName(string tcFullName)
        {
            return _testCaseIds.TryGetValue(tcFullName, out var tcId) ? tcId : null;
        }

        private int ConvertNUnitResultAndErrorFlagToStatusCode(string nunitResult, string nunitTestId)
        {
            if (!string.IsNullOrWhiteSpace(nunitTestId) &&
                _testCasesWithErrors.TryGetValue(nunitTestId, out var hasError) && hasError)
            {
                return Protocol.STATUS_FAILED;  // Error maps to failed
            }

            if (string.IsNullOrEmpty(nunitResult)) return Protocol.STATUS_FAILED;

            switch (nunitResult.ToLower())
            {
                case "passed": return Protocol.STATUS_PASSED;
                case "failed": return Protocol.STATUS_FAILED;
                case "skipped": return Protocol.STATUS_SKIPPED;
                case "inconclusive": return Protocol.STATUS_FAILED;
                default:
                    ThreadSafeFileLogger.LogWebSocketConnectionFailed($"Unexpected NUnit result: {nunitResult}, defaulting to failed");
                    return Protocol.STATUS_FAILED;
            }
        }

        /// <summary>
        /// Queue an exception event for batched sending.
        /// </summary>
        public void QueueException(
            string nunitTestId,
            string message,
            string stackTrace,
            string exceptionType = null,
            IEnumerable<string> lines = null,
            bool isError = false,
            string timestamp = null)
        {
            if (string.IsNullOrEmpty(_runId) || string.IsNullOrWhiteSpace(nunitTestId) || string.IsNullOrWhiteSpace(stackTrace))
                return;

            if (isError)
            {
                _testCasesWithErrors[nunitTestId] = true;
            }

            string tcId = nunitTestId;
            if (string.IsNullOrEmpty(tcId)) return;

            var normalizedLines = lines != null
                ? new List<string>(lines)
                : new List<string>(stackTrace.Replace("\r\n", "\n").Split(new[] { '\n' }, StringSplitOptions.RemoveEmptyEntries));

            // Parse timestamp if provided as ISO string, otherwise use current time
            long tsMs;
            if (!string.IsNullOrEmpty(timestamp) && DateTime.TryParse(timestamp, out var dt))
                tsMs = Protocol.ToMs(dt);
            else
                tsMs = Protocol.NowMs();

            var eventData = new Dictionary<string, object>
            {
                { Protocol.F_EVENT_TYPE, Protocol.MSG_EXCEPTION },
                { Protocol.F_TC_ID, tcId },
                { Protocol.F_TIMESTAMP, tsMs },
                { Protocol.F_MESSAGE, message },
                { Protocol.F_EXCEPTION_TYPE, exceptionType },
                { Protocol.F_IS_ERROR, isError },
                { Protocol.F_STACK_TRACE, normalizedLines }
            };

            QueueEvent(new BatchEvent { EventType = Protocol.MSG_EXCEPTION, Data = eventData, TestCaseId = tcId });
        }

        /// <summary>
        /// Async method for sending exception (queues for batching).
        /// </summary>
        public Task SendExceptionAsync(
            string nunitTestId,
            string message,
            string stackTrace,
            string exceptionType = null,
            IEnumerable<string> lines = null,
            bool isError = false,
            string timestamp = null)
        {
            QueueException(nunitTestId, message, stackTrace, exceptionType, lines, isError, timestamp);
            return Task.CompletedTask;
        }

        public void SendException(
            string nunitTestId,
            string message,
            string stackTrace,
            string exceptionType = null,
            IEnumerable<string> lines = null,
            bool isError = false,
            string timestamp = null)
        {
            QueueException(nunitTestId, message, stackTrace, exceptionType, lines, isError, timestamp);
        }

        /// <summary>
        /// Queue a log message for batched sending.
        /// </summary>
        public void QueueLogMessage(string message, string component, string channel = null, string dir = null, string nunitTestId = null, string timestamp = null, string phase = null)
        {
            if (string.IsNullOrEmpty(_runId) || string.IsNullOrEmpty(message)) return;

            if (!string.IsNullOrEmpty(dir) && dir != "tx" && dir != "rx")
            {
                throw new ArgumentException("dir must be 'tx' or 'rx' if provided", nameof(dir));
            }

            // Parse timestamp if provided as ISO string, otherwise use current time
            long tsMs;
            if (!string.IsNullOrEmpty(timestamp) && DateTime.TryParse(timestamp, out var dt))
                tsMs = Protocol.ToMs(dt);
            else
                tsMs = Protocol.NowMs();

            var logEntry = new LogEntry
            {
                TimestampMs = tsMs,
                Message = message,
                ComponentRaw = component ?? Environment.MachineName,
                ChannelRaw = channel,
                DirCode = Protocol.DirToCode(dir),
                PhaseCode = Protocol.PhaseToCode(phase)
            };

            // Create or add to existing log batch event for this test case
            QueueLogEntry(nunitTestId, logEntry);
        }

        /// <summary>
        /// Queue a simple message.
        /// </summary>
        public void QueueMessage(string message, string source = "console", string testCaseId = null)
        {
            QueueMessage(message, source, testCaseId, DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"));
        }

        public void QueueMessage(string message, string source, string testCaseId, string timestamp)
        {
            QueueLogMessage(message, Environment.MachineName, source, null, testCaseId, timestamp);
        }

        private void QueueLogEntry(string nunitTestId, LogEntry entry)
        {
            string tcId = nunitTestId ?? "global";

            var batchEvent = new BatchEvent
            {
                EventType = Protocol.MSG_LOG_BATCH,
                TestCaseId = tcId,
                LogEntry = entry
            };

            QueueEvent(batchEvent);
        }

        private void QueueEvent(BatchEvent evt)
        {
            _eventQueue.Enqueue(evt);

            // Estimate size and trigger flush if buffer is getting large
            var estimatedSize = Interlocked.Add(ref _estimatedBatchSize, EstimateEventSize(evt));
            if (estimatedSize >= MaxBatchSizeBytes)
            {
                // Fire and forget async flush
                _ = Task.Run(async () => await FlushBatchAsync());
            }
        }

        private static int EstimateEventSize(BatchEvent evt)
        {
            // Rough estimate: base overhead + message content
            int size = 50; // Base MessagePack overhead (smaller than JSON)
            if (evt.LogEntry != null)
            {
                size += (evt.LogEntry.Message?.Length ?? 0) + 30;
            }
            else if (evt.Data != null)
            {
                size += 150; // Estimated size for other event types
            }
            return size;
        }

        private void OnBatchTimerTick(object state)
        {
            // Don't block the timer thread - use a dedicated task
            _ = Task.Run(async () => await FlushBatchAsync());
        }

        private void OnHeartbeatTimerTick(object state)
        {
            // Send heartbeat if no message has been sent recently
            _ = Task.Run(async () => await SendHeartbeatIfNeededAsync());
        }

        /// <summary>
        /// Send a heartbeat message to keep the connection alive.
        /// </summary>
        private async Task SendHeartbeatIfNeededAsync()
        {
            if (string.IsNullOrEmpty(_runId)) return;
            if (_webSocket?.State != WebSocketState.Open) return;

            var timeSinceLastSend = DateTime.UtcNow - _lastSendTime;
            if (timeSinceLastSend.TotalMilliseconds < HeartbeatIntervalMs - 1000)
            {
                // Recent activity, no heartbeat needed
                return;
            }

            try
            {
                var heartbeat = new Dictionary<string, object>
                {
                    { Protocol.F_TYPE, Protocol.MSG_HEARTBEAT },
                    { Protocol.F_RUN_ID, _runId }
                };
                await SendWebSocketMessage(heartbeat);
                ThreadSafeFileLogger.Log($"Heartbeat sent (last activity {timeSinceLastSend.TotalSeconds:F1}s ago)");
            }
            catch (Exception ex)
            {
                ThreadSafeFileLogger.LogWebSocketException("Heartbeat", ex);
            }
        }

        private void OnMetricsTimerTick(object state)
        {
            _ = Task.Run(async () => await FlushMetricsAsync());
        }

        /// <summary>
        /// Start system metrics collection.
        /// </summary>
        private void StartMetricsCollection()
        {
            try
            {
                _metricsCollector.Start();
                // Send metrics every 5 seconds (batches 5 samples)
                _metricsTimer.Change(5000, 5000);
                ThreadSafeFileLogger.Log("Metrics collection started");
            }
            catch (Exception ex)
            {
                ThreadSafeFileLogger.Log($"Failed to start metrics collection: {ex.Message}");
            }
        }

        /// <summary>
        /// Stop system metrics collection.
        /// </summary>
        private void StopMetricsCollection()
        {
            try
            {
                _metricsTimer.Change(Timeout.Infinite, Timeout.Infinite);
                _metricsCollector.Stop();
                ThreadSafeFileLogger.Log("Metrics collection stopped");
            }
            catch (Exception ex)
            {
                ThreadSafeFileLogger.Log($"Error stopping metrics collection: {ex.Message}");
            }
        }

        /// <summary>
        /// Flush collected metrics samples to the server.
        /// </summary>
        private async Task FlushMetricsAsync()
        {
            if (string.IsNullOrEmpty(_runId)) return;
            if (_webSocket?.State != WebSocketState.Open) return;

            try
            {
                var samples = _metricsCollector.DrainSamples();
                if (samples.Length == 0) return;

                var metricsList = new List<Dictionary<string, object>>();
                foreach (var sample in samples)
                {
                    var sampleDict = new Dictionary<string, object>
                    {
                        { Protocol.F_TIMESTAMP, sample.TimestampMs },
                        { Protocol.F_CPU, sample.CpuPercent },
                        { Protocol.F_MEMORY, sample.MemoryPercent },
                        { Protocol.F_NET, sample.NetPercent }
                    };

                    // Include per-interface details if available
                    if (sample.NetworkInterfaces != null && sample.NetworkInterfaces.Length > 0)
                    {
                        var niList = new List<Dictionary<string, object>>();
                        foreach (var ni in sample.NetworkInterfaces)
                        {
                            niList.Add(new Dictionary<string, object>
                            {
                                { "n", ni.Name },
                                { "tx", ni.TxPercent },
                                { "rx", ni.RxPercent }
                            });
                        }
                        sampleDict[Protocol.F_NET_INTERFACES] = niList;
                    }

                    metricsList.Add(sampleDict);
                }

                var metricsMsg = new Dictionary<string, object>
                {
                    { Protocol.F_TYPE, Protocol.MSG_METRICS },
                    { Protocol.F_RUN_ID, _runId },
                    { Protocol.F_METRICS, metricsList }
                };

                await SendWebSocketMessage(metricsMsg);
                ThreadSafeFileLogger.Log($"Sent {samples.Length} metrics samples");
            }
            catch (Exception ex)
            {
                ThreadSafeFileLogger.LogWebSocketException("FlushMetrics", ex);
            }
        }

        /// <summary>
        /// Flush all pending events as a batch message.
        /// </summary>
        public async Task FlushBatchAsync()
        {
            if (_eventQueue.IsEmpty || string.IsNullOrEmpty(_runId)) return;

            await _sendSemaphore.WaitAsync();
            try
            {
                // Collect all events (up to max)
                var events = new List<object>();
                var logBatches = new Dictionary<string, List<LogEntry>>();

                int count = 0;
                while (count < MaxEventsPerBatch && _eventQueue.TryDequeue(out var evt))
                {
                    count++;

                    // Consolidate log entries for the same test case
                    if (evt.EventType == Protocol.MSG_LOG_BATCH && evt.LogEntry != null)
                    {
                        var tcId = evt.TestCaseId ?? "global";
                        if (!logBatches.TryGetValue(tcId, out var entries))
                        {
                            entries = new List<LogEntry>();
                            logBatches[tcId] = entries;
                        }
                        entries.Add(evt.LogEntry);
                    }
                    else
                    {
                        events.Add(evt.Data);
                    }
                }

                // Add consolidated log batches with string interning
                foreach (var kvp in logBatches)
                {
                    var entriesList = new List<Dictionary<string, object>>();
                    foreach (var entry in kvp.Value)
                    {
                        var entryDict = new Dictionary<string, object>
                        {
                            { Protocol.F_TIMESTAMP, entry.TimestampMs },
                            { Protocol.F_MESSAGE, entry.Message }
                        };

                        // Use interned strings for component/channel
                        var componentInterned = GetInternedString(entry.ComponentRaw);
                        if (componentInterned != null)
                            entryDict[Protocol.F_COMPONENT] = componentInterned;

                        var channelInterned = GetInternedString(entry.ChannelRaw);
                        if (channelInterned != null)
                            entryDict[Protocol.F_CHANNEL] = channelInterned;

                        if (entry.DirCode.HasValue)
                            entryDict[Protocol.F_DIR] = entry.DirCode.Value;

                        if (entry.PhaseCode.HasValue)
                            entryDict[Protocol.F_PHASE] = entry.PhaseCode.Value;

                        entriesList.Add(entryDict);
                    }

                    events.Add(new Dictionary<string, object>
                    {
                        { Protocol.F_EVENT_TYPE, Protocol.MSG_LOG_BATCH },
                        { Protocol.F_TC_ID, kvp.Key },
                        { Protocol.F_ENTRIES, entriesList }
                    });
                }

                Interlocked.Exchange(ref _estimatedBatchSize, 0);

                if (events.Count == 0) return;

                var batchMessage = new Dictionary<string, object>
                {
                    { Protocol.F_TYPE, Protocol.MSG_BATCH },
                    { Protocol.F_RUN_ID, _runId },
                    { Protocol.F_EVENTS, events }
                };

                if (await SendWebSocketMessageDirect(batchMessage))
                    _lastSendTime = DateTime.UtcNow;
                else
                    Interlocked.Add(ref _droppedEventCount, count);
            }
            finally
            {
                _sendSemaphore.Release();
            }
        }

        /// <summary>
        /// Flush events for a specific test case (used before test_case_finished).
        /// </summary>
        private async Task FlushEventsForTestCaseAsync(string nunitTestId)
        {
            if (string.IsNullOrEmpty(nunitTestId)) return;

            // First do a general flush to clear the queue
            await FlushBatchAsync();

            // Small delay to allow any in-flight operations
            await Task.Delay(10);
        }

        private async Task SendWebSocketMessage(object data)
        {
            await _sendSemaphore.WaitAsync();
            try
            {
                await SendWebSocketMessageDirect(data);
                _lastSendTime = DateTime.UtcNow;
            }
            finally
            {
                _sendSemaphore.Release();
            }
        }

        private async Task<bool> SendWebSocketMessageDirect(object data)
        {
            var messageType = GetMessageType(data);
            var messageId = Interlocked.Increment(ref _messageSequence);

            if (_webSocket?.State != WebSocketState.Open)
            {
                var stateSnapshot = DescribeSocketState();
                ThreadSafeFileLogger.LogWebSocketNotOpen(stateSnapshot);
                ThreadSafeFileLogger.LogSendFailed($"Send[{messageId}] skipped (type={messageType})");
                return false;
            }

            byte[] bytes;
            try
            {
                bytes = MessagePackSerializer.Serialize(data, _msgpackOptions);
            }
            catch (Exception ex)
            {
                ThreadSafeFileLogger.LogWebSocketException($"Serialize message {messageId} ({messageType})", ex);
                return false;
            }

            ThreadSafeFileLogger.LogSendStarting(messageId, messageType, DescribeSocketState());

            try
            {
                // A timed-out send aborts the socket, so later sends fail fast instead of blocking tests.
                using (var timeout = new CancellationTokenSource(SendTimeout))
                {
                    await _webSocket.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Binary, true, timeout.Token);
                }
                ThreadSafeFileLogger.LogSendCompleted(messageId, messageType, bytes.Length);
                return true;
            }
            catch (Exception ex)
            {
                ThreadSafeFileLogger.LogSendFailed(ex.Message);
                ThreadSafeFileLogger.LogWebSocketException($"Send[{messageId}] ({messageType})", ex);
                return false;
            }
        }

        /// <summary>
        /// Flushes every queued event, sending as many batches as needed.
        /// </summary>
        public async Task FlushAllMessagesAsync()
        {
            while (!_eventQueue.IsEmpty && !string.IsNullOrEmpty(_runId))
            {
                await FlushBatchAsync();
            }
        }

        /// <summary>
        /// Flushes all messages synchronously.
        /// </summary>
        public void FlushAllMessages()
        {
            FlushAllMessagesAsync().Wait();
        }

        /// <summary>
        /// Waits for pending operations to complete.
        /// </summary>
        public async Task WaitForFlushCompleteAsync()
        {
            await FlushAllMessagesAsync();
        }

        private void RecordKpiUploadFailure(string reason)
        {
            _kpiUploadFailure = reason;
            ThreadSafeFileLogger.LogWebSocketConnectionFailed($"KPI upload failed for run {_runId}: {reason}");
        }

        private static async Task<string> ReadErrorDetailAsync(HttpResponseMessage response)
        {
            try
            {
                var body = await response.Content.ReadAsStringAsync();
                if (string.IsNullOrWhiteSpace(body)) return "";
                body = body.Trim();
                return ": " + (body.Length > 300 ? body.Substring(0, 300) : body);
            }
            catch
            {
                return "";
            }
        }

        /// <summary>
        /// Uploads an attachment for a test case (synchronous version).
        /// </summary>
        public bool UploadAttachment(string nunitTestId, string filePath, string description = null)
        {
            return UploadAttachmentAsync(nunitTestId, filePath, description).GetAwaiter().GetResult();
        }

        private void WriteUrlFile(string filePath, string url)
        {
            try
            {
                var directory = Path.GetDirectoryName(filePath);
                if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }
                File.WriteAllText(filePath, url);
                ThreadSafeFileLogger.LogMessageSent($"Wrote URL file: {filePath}");
            }
            catch (Exception ex)
            {
                ThreadSafeFileLogger.LogWebSocketConnectionFailed($"Failed to write URL file {filePath}: {ex.Message}");
            }
        }

        /// <summary>
        /// Uploads an attachment for a test case.
        /// </summary>
        public async Task<bool> UploadAttachmentAsync(string nunitTestId, string filePath, string description = null)
        {
            try
            {
                if (!File.Exists(filePath))
                {
                    ThreadSafeFileLogger.LogWebSocketConnectionFailed($"Attachment file not found: {filePath}");
                    return false;
                }

                string tcId = GetTcIdFromTestId(nunitTestId) ?? GetTcIdFromFullName(nunitTestId);
                if (tcId == null)
                {
                    ThreadSafeFileLogger.LogWebSocketConnectionFailed($"tc_id not found for test case: {nunitTestId}");
                    return false;
                }

                var fileName = Path.GetFileName(filePath);
                var uploadUrl = $"{_serverBaseUrl}/api/attachments/{_runId}/{tcId}/upload";

                using (var form = new MultipartFormDataContent())
                using (var fileContent = new ByteArrayContent(await ReadAllBytesAsync(filePath)))
                {
                    fileContent.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/octet-stream");
                    form.Add(fileContent, "attachment", fileName);

                    if (!string.IsNullOrEmpty(description))
                    {
                        form.Add(new StringContent(description), "description");
                    }

                    var response = await _httpClient.PostAsync(uploadUrl, form);

                    if (response.IsSuccessStatusCode)
                    {
                        return true;
                    }
                    else
                    {
                        var errorContent = await response.Content.ReadAsStringAsync();
                        ThreadSafeFileLogger.LogWebSocketConnectionFailed($"Attachment upload failed: {response.StatusCode} - {errorContent}");
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                ThreadSafeFileLogger.LogWebSocketConnectionFailed($"Attachment upload error: {ex.Message}");
                return false;
            }
        }

        private static async Task<byte[]> ReadAllBytesAsync(string path)
        {
#if NET462
            return await Task.Run(() => File.ReadAllBytes(path));
#else
            return await File.ReadAllBytesAsync(path);
#endif
        }

        /// <summary>
        /// Gracefully close the WebSocket connection with a normal close handshake.
        /// Should be called after SendRunFinished() to ensure the server receives all messages.
        /// </summary>
        public async Task CloseAsync()
        {
            if (_webSocket != null && _webSocket.State == WebSocketState.Open)
            {
                try
                {
                    ThreadSafeFileLogger.Log("Initiating graceful WebSocket close");
                    using var cts = new CancellationTokenSource(CloseTimeout);
                    await _webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Run finished", cts.Token);
                    ThreadSafeFileLogger.Log("WebSocket closed gracefully");
                }
                catch (OperationCanceledException)
                {
                    ThreadSafeFileLogger.Log("WebSocket close timed out after 5 seconds");
                }
                catch (Exception ex)
                {
                    ThreadSafeFileLogger.Log($"Error during WebSocket close: {ex.Message}");
                }
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                StopPassiveReceiveLoop();
                _metricsCollector?.Dispose();
                _metricsTimer?.Dispose();
                _batchTimer?.Dispose();
                _heartbeatTimer?.Dispose();
                ThreadSafeFileLogger.Log("Disposing WebSocketHelper");
                if (_webSocket != null)
                {
                    ThreadSafeFileLogger.LogWebSocketStateDetails("Dispose", _webSocket.State, _webSocket.CloseStatus, _webSocket.CloseStatusDescription);
                    // Try graceful close if still open
                    if (_webSocket.State == WebSocketState.Open)
                    {
                        try
                        {
                            ThreadSafeFileLogger.Log("Attempting graceful close during Dispose");
                            using (var closeTimeout = new CancellationTokenSource(CloseTimeout))
                            {
                                _webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Disposing", closeTimeout.Token)
                                    .GetAwaiter().GetResult();
                            }
                        }
                        catch (Exception ex)
                        {
                            ThreadSafeFileLogger.Log($"Graceful close failed during Dispose: {ex.Message}");
                        }
                    }
                }
                _webSocket?.Dispose();
                _httpClient?.Dispose();
                _sendSemaphore?.Dispose();
                _disposed = true;
            }
        }

        /// <summary>
        /// Safely converts a MessagePack-deserialized value to int.
        /// MessagePack uses minimal encoding, so small integers may be byte, sbyte, short, etc.
        /// </summary>
        private static bool TryGetInt(object value, out int result)
        {
            result = 0;
            if (value == null) return false;

            try
            {
                result = Convert.ToInt32(value);
                return true;
            }
            catch
            {
                return false;
            }
        }

        private static string GetMessageType(object data)
        {
            if (data == null) return "null";

            if (data is IDictionary<string, object> dict &&
                dict.TryGetValue(Protocol.F_TYPE, out var typeValue) &&
                typeValue != null)
            {
                return typeValue.ToString();
            }

            return "unknown";
        }

        private string DescribeSocketState()
        {
            if (_webSocket == null) return "null";

            var closeStatus = _webSocket.CloseStatus?.ToString() ?? "null";
            var closeDescription = _webSocket.CloseStatusDescription ?? "null";
            return $"State={_webSocket.State}, CloseStatus={closeStatus}, CloseDescription={closeDescription}";
        }
    }

    /// <summary>
    /// Internal class representing a queued event for batching.
    /// </summary>
    internal class BatchEvent
    {
        public int EventType { get; set; }
        public Dictionary<string, object> Data { get; set; }
        public string TestCaseId { get; set; }
        public LogEntry LogEntry { get; set; }  // For log_batch events, store the entry directly
    }

    /// <summary>
    /// Log entry for WebSocket messages using optimized binary format.
    /// Stores raw values; string interning is applied during batch serialization.
    /// </summary>
    internal class LogEntry
    {
        /// <summary>Timestamp in milliseconds since Unix epoch.</summary>
        public long TimestampMs { get; set; }

        /// <summary>Log message text.</summary>
        public string Message { get; set; }

        /// <summary>Raw component name (will be interned during serialization).</summary>
        public string ComponentRaw { get; set; }

        /// <summary>Raw channel name (will be interned during serialization).</summary>
        public string ChannelRaw { get; set; }

        /// <summary>Direction code (1=tx, 2=rx) or null if not specified.</summary>
        public int? DirCode { get; set; }

        /// <summary>Phase code (1=teardown) or null if not specified.</summary>
        public int? PhaseCode { get; set; }
    }
}
