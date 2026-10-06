using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net.NetworkInformation;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

[assembly: InternalsVisibleTo("TestRift.NUnit.Tests")]
[assembly: InternalsVisibleTo("TestRift.NUnit.Tests.MaxVersions")]

namespace TestRift.NUnit
{
    /// <summary>
    /// Lightweight system metrics collector that samples CPU, memory, and network usage.
    /// Uses efficient methods to minimize performance impact during test execution.
    /// Automatically downsamples when too many samples accumulate (for long test runs).
    /// </summary>
    internal class SystemMetricsCollector : IDisposable
    {
        private readonly int _intervalMs;
        private readonly object _samplesLock = new object();
        private List<MetricSample> _samples = new List<MetricSample>();
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private Task _collectionTask;
        private bool _disposed;

        // For system-wide CPU calculation
        private long _lastIdleTime;
        private long _lastKernelTime;
        private long _lastUserTime;

        // For network interface tracking (max 2 interfaces)
        private NetworkInterfaceInfo[] _networkInterfaces;
        private const int MaxNetworkInterfaces = 2;

        // Downsampling: keep at most this many samples
        private const int MaxSamples = 2000;
        // Track effective interval after downsampling
        private int _effectiveIntervalMs;

        /// <summary>
        /// Creates a new metrics collector with the specified sampling interval.
        /// </summary>
        /// <param name="intervalMs">Sampling interval in milliseconds (default 1000ms).</param>
        public SystemMetricsCollector(int intervalMs = 1000)
        {
            _intervalMs = intervalMs;
            _effectiveIntervalMs = intervalMs;
        }

        /// <summary>
        /// Starts the metrics collection in a background task.
        /// </summary>
        public void Start()
        {
            if (_collectionTask != null)
                return;

            // Initialize system CPU tracking
            GetSystemTimesValues(out _lastIdleTime, out _lastKernelTime, out _lastUserTime);

            // Initialize network interface tracking
            InitializeNetworkInterfaces();

            _collectionTask = Task.Run(async () => await CollectLoopAsync(_cts.Token));
        }

        /// <summary>
        /// Stops the metrics collection.
        /// </summary>
        public void Stop()
        {
            if (_collectionTask == null)
                return;

            _cts.Cancel();
            try
            {
                _collectionTask.Wait(TimeSpan.FromSeconds(2));
            }
            catch (AggregateException)
            {
                // Expected when cancelled
            }
            _collectionTask = null;
        }

        /// <summary>
        /// Drains all pending samples and returns them as an array.
        /// Returns downsampled data if the run has been going long.
        /// </summary>
        public MetricSample[] DrainSamples()
        {
            lock (_samplesLock)
            {
                if (_samples.Count == 0)
                    return Array.Empty<MetricSample>();

                var result = _samples.ToArray();
                _samples.Clear();
                return result;
            }
        }

        /// <summary>
        /// Gets the current effective interval in milliseconds.
        /// May be larger than the configured interval if downsampling has occurred.
        /// </summary>
        public int EffectiveIntervalMs => _effectiveIntervalMs;

        private async Task CollectLoopAsync(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    var sample = CollectSample();
                    if (sample != null)
                    {
                        AddSampleWithDownsampling(sample);
                    }

                    await Task.Delay(_intervalMs, token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    ThreadSafeFileLogger.Log($"Metrics collection error: {ex.Message}");
                    await Task.Delay(_intervalMs, token);
                }
            }
        }

        private void AddSampleWithDownsampling(MetricSample sample)
        {
            lock (_samplesLock)
            {
                _samples.Add(sample);

                // If we exceed max samples, downsample by averaging pairs
                if (_samples.Count > MaxSamples)
                {
                    var downsampled = new List<MetricSample>(_samples.Count / 2 + 1);
                    for (int i = 0; i < _samples.Count - 1; i += 2)
                    {
                        var s1 = _samples[i];
                        var s2 = _samples[i + 1];
                        var ds = new MetricSample
                        {
                            // Use midpoint timestamp
                            TimestampMs = (s1.TimestampMs + s2.TimestampMs) / 2,
                            CpuPercent = Math.Round((s1.CpuPercent + s2.CpuPercent) / 2, 1),
                            MemoryPercent = Math.Round((s1.MemoryPercent + s2.MemoryPercent) / 2, 1),
                            NetPercent = Math.Round((s1.NetPercent + s2.NetPercent) / 2, 1)
                        };
                        // Average network interface data
                        if (s1.NetworkInterfaces != null && s2.NetworkInterfaces != null)
                        {
                            var netList = new List<NetworkInterfaceMetric>();
                            for (int n = 0; n < Math.Min(s1.NetworkInterfaces.Length, s2.NetworkInterfaces.Length); n++)
                            {
                                netList.Add(new NetworkInterfaceMetric
                                {
                                    Name = s1.NetworkInterfaces[n].Name,
                                    TxPercent = Math.Round((s1.NetworkInterfaces[n].TxPercent + s2.NetworkInterfaces[n].TxPercent) / 2, 1),
                                    RxPercent = Math.Round((s1.NetworkInterfaces[n].RxPercent + s2.NetworkInterfaces[n].RxPercent) / 2, 1)
                                });
                            }
                            ds.NetworkInterfaces = netList.ToArray();
                        }
                        downsampled.Add(ds);
                    }
                    // Handle odd count - keep last sample
                    if (_samples.Count % 2 == 1)
                    {
                        downsampled.Add(_samples[_samples.Count - 1]);
                    }

                    _samples = downsampled;
                    _effectiveIntervalMs *= 2;
                    ThreadSafeFileLogger.Log($"Metrics downsampled to {_samples.Count} samples, effective interval now {_effectiveIntervalMs}ms");
                }
            }
        }

        private MetricSample CollectSample()
        {
            try
            {
                // Calculate system-wide CPU usage
                GetSystemTimesValues(out long idleTime, out long kernelTime, out long userTime);

                long idleDelta = idleTime - _lastIdleTime;
                long kernelDelta = kernelTime - _lastKernelTime;
                long userDelta = userTime - _lastUserTime;

                // Total time = kernel + user (kernel includes idle time on Windows)
                long totalDelta = kernelDelta + userDelta;
                // Busy time = total - idle
                long busyDelta = totalDelta - idleDelta;

                double cpuPercent = 0;
                if (totalDelta > 0)
                {
                    cpuPercent = (busyDelta * 100.0) / totalDelta;
                    cpuPercent = Math.Min(100, Math.Max(0, cpuPercent)); // Clamp to 0-100
                }

                _lastIdleTime = idleTime;
                _lastKernelTime = kernelTime;
                _lastUserTime = userTime;

                // Memory: use available memory as percentage of total physical memory
                var totalMemory = GetTotalPhysicalMemory();
                var availableMemory = GetAvailablePhysicalMemory();
                double memoryPercent = totalMemory > 0 
                    ? ((totalMemory - availableMemory) / (double)totalMemory) * 100.0 
                    : 0;
                memoryPercent = Math.Min(100, Math.Max(0, memoryPercent)); // Clamp to 0-100

                // Collect network metrics
                var netMetrics = CollectNetworkMetrics();
                double netPercent = 0;
                if (netMetrics != null && netMetrics.Length > 0)
                {
                    // Find max utilization across all interfaces (TX or RX)
                    foreach (var nm in netMetrics)
                    {
                        netPercent = Math.Max(netPercent, Math.Max(nm.TxPercent, nm.RxPercent));
                    }
                }

                return new MetricSample
                {
                    TimestampMs = Protocol.NowMs(),
                    CpuPercent = Math.Round(cpuPercent, 1),
                    MemoryPercent = Math.Round(memoryPercent, 1),
                    NetPercent = Math.Round(netPercent, 1),
                    NetworkInterfaces = netMetrics
                };
            }
            catch (Exception ex)
            {
                ThreadSafeFileLogger.Log($"Error collecting metrics sample: {ex.Message}");
                return null;
            }
        }

        private void InitializeNetworkInterfaces()
        {
            try
            {
                var interfaces = NetworkInterface.GetAllNetworkInterfaces();
                var selected = new List<NetworkInterfaceInfo>();

                foreach (var ni in interfaces)
                {
                    // Skip non-operational interfaces
                    if (ni.OperationalStatus != OperationalStatus.Up)
                        continue;

                    // Skip loopback
                    if (ni.NetworkInterfaceType == NetworkInterfaceType.Loopback)
                        continue;

                    // Skip tunnel/VPN types
                    if (ni.NetworkInterfaceType == NetworkInterfaceType.Tunnel)
                        continue;

                    // Skip virtual interfaces by name pattern
                    var name = ni.Name.ToLowerInvariant();
                    var desc = ni.Description.ToLowerInvariant();
                    if (name.Contains("docker") || name.Contains("vethernet") ||
                        name.Contains("vmware") || name.Contains("virtualbox") ||
                        name.Contains("vpn") || name.Contains("loopback") ||
                        desc.Contains("virtual") || desc.Contains("vmware") ||
                        desc.Contains("virtualbox") || desc.Contains("hyper-v") ||
                        desc.Contains("vpn") || desc.Contains("docker"))
                        continue;

                    // Skip interfaces with no speed info (usually virtual)
                    if (ni.Speed <= 0)
                        continue;

                    var stats = ni.GetIPStatistics();
                    selected.Add(new NetworkInterfaceInfo
                    {
                        InterfaceId = ni.Id,
                        Name = ni.Name.Length > 12 ? ni.Name.Substring(0, 12) : ni.Name,
                        SpeedBps = ni.Speed,
                        LastBytesSent = stats.BytesSent,
                        LastBytesReceived = stats.BytesReceived,
                        LastSampleTime = DateTime.UtcNow
                    });

                    if (selected.Count >= MaxNetworkInterfaces)
                        break;
                }

                _networkInterfaces = selected.ToArray();
                if (_networkInterfaces.Length > 0)
                {
                    foreach (var iface in _networkInterfaces)
                    {
                        ThreadSafeFileLogger.Log($"Tracking network interface: {iface.Name} (ID: {iface.InterfaceId}), Speed: {iface.SpeedBps / 1_000_000} Mbps");
                    }
                }
                else
                {
                    ThreadSafeFileLogger.Log("No suitable network interfaces found for tracking");
                }
            }
            catch (Exception ex)
            {
                ThreadSafeFileLogger.Log($"Failed to initialize network interfaces: {ex.Message}");
                _networkInterfaces = Array.Empty<NetworkInterfaceInfo>();
            }
        }

        private NetworkInterfaceMetric[] CollectNetworkMetrics()
        {
            if (_networkInterfaces == null || _networkInterfaces.Length == 0)
                return null;

            // Get fresh interface list to get updated stats
            var freshInterfaces = NetworkInterface.GetAllNetworkInterfaces();
            var now = DateTime.UtcNow;
            var results = new List<NetworkInterfaceMetric>();

            foreach (var info in _networkInterfaces)
            {
                try
                {
                    // Find the matching fresh interface by ID
                    NetworkInterface freshNi = null;
                    foreach (var ni in freshInterfaces)
                    {
                        if (ni.Id == info.InterfaceId)
                        {
                            freshNi = ni;
                            break;
                        }
                    }
                    if (freshNi == null) continue;

                    var stats = freshNi.GetIPStatistics();
                    var elapsedSeconds = (now - info.LastSampleTime).TotalSeconds;
                    if (elapsedSeconds <= 0)
                        elapsedSeconds = 1;

                    var bytesSentDelta = stats.BytesSent - info.LastBytesSent;
                    var bytesReceivedDelta = stats.BytesReceived - info.LastBytesReceived;

                    // Convert to bits per second
                    var txBps = (bytesSentDelta * 8) / elapsedSeconds;
                    var rxBps = (bytesReceivedDelta * 8) / elapsedSeconds;

                    // Calculate utilization as percentage of link speed
                    double txPercent = info.SpeedBps > 0 ? (txBps / info.SpeedBps) * 100.0 : 0;
                    double rxPercent = info.SpeedBps > 0 ? (rxBps / info.SpeedBps) * 100.0 : 0;

                    // Clamp to 0-100 (can exceed if speed is incorrect)
                    txPercent = Math.Min(100, Math.Max(0, txPercent));
                    rxPercent = Math.Min(100, Math.Max(0, rxPercent));

                    results.Add(new NetworkInterfaceMetric
                    {
                        Name = info.Name,
                        TxPercent = Math.Round(txPercent, 1),
                        RxPercent = Math.Round(rxPercent, 1)
                    });

                    // Update for next sample
                    info.LastBytesSent = stats.BytesSent;
                    info.LastBytesReceived = stats.BytesReceived;
                    info.LastSampleTime = now;
                }
                catch
                {
                    // Skip this interface on error
                }
            }

            return results.Count > 0 ? results.ToArray() : null;
        }

        private static void GetSystemTimesValues(out long idleTime, out long kernelTime, out long userTime)
        {
            idleTime = 0;
            kernelTime = 0;
            userTime = 0;

#if NETFRAMEWORK
            if (GetSystemTimes(out var idle, out var kernel, out var user))
            {
                idleTime = idle.ToInt64();
                kernelTime = kernel.ToInt64();
                userTime = user.ToInt64();
            }
#else
            // For .NET Core/.NET 5+, also use P/Invoke on Windows
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                if (GetSystemTimesCore(out var idle, out var kernel, out var user))
                {
                    idleTime = idle;
                    kernelTime = kernel;
                    userTime = user;
                }
            }
            else
            {
                TryReadProcStatTimes(out idleTime, out kernelTime, out userTime);
            }
#endif
        }

        internal static bool TryParseProcStatTimes(string procStat, out long idleTime, out long kernelTime, out long userTime)
        {
            idleTime = 0;
            kernelTime = 0;
            userTime = 0;
            if (string.IsNullOrEmpty(procStat))
                return false;

            using (var reader = new StringReader(procStat))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    if (!line.StartsWith("cpu ", StringComparison.Ordinal))
                        continue;

                    var parts = line.Split((char[])null, StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length < 5)
                        return false;

                    // user nice system idle [iowait irq softirq steal guest guest_nice]
                    long user = ParseProcLong(parts[1]);
                    long nice = ParseProcLong(parts[2]);
                    long system = ParseProcLong(parts[3]);
                    long idle = ParseProcLong(parts[4]);
                    long iowait = parts.Length > 5 ? ParseProcLong(parts[5]) : 0;
                    long irq = parts.Length > 6 ? ParseProcLong(parts[6]) : 0;
                    long softirq = parts.Length > 7 ? ParseProcLong(parts[7]) : 0;
                    long steal = parts.Length > 8 ? ParseProcLong(parts[8]) : 0;

                    // Match Windows GetSystemTimes: idle includes iowait, kernel
                    // includes idle, user is user+nice. Busy = (kernel+user)-idle.
                    idleTime = idle + iowait;
                    kernelTime = system + irq + softirq + steal + idleTime;
                    userTime = user + nice;
                    return true;
                }
            }

            return false;
        }

        private static bool TryReadProcStatTimes(out long idleTime, out long kernelTime, out long userTime)
        {
            idleTime = 0;
            kernelTime = 0;
            userTime = 0;
            try
            {
                return TryParseProcStatTimes(File.ReadAllText("/proc/stat"), out idleTime, out kernelTime, out userTime);
            }
            catch
            {
                return false;
            }
        }

        private static long ParseProcLong(string value)
        {
            return long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : 0;
        }

        private static long GetAvailablePhysicalMemory()
        {
#if NETFRAMEWORK
            var memStatus = new MEMORYSTATUSEX();
            memStatus.dwLength = (uint)Marshal.SizeOf(typeof(MEMORYSTATUSEX));
            if (GlobalMemoryStatusEx(ref memStatus))
            {
                return (long)memStatus.ullAvailPhys;
            }
            return 0;
#else
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                var memStatus = new MEMORYSTATUSEX();
                memStatus.dwLength = (uint)Marshal.SizeOf(typeof(MEMORYSTATUSEX));
                if (GlobalMemoryStatusEx(ref memStatus))
                {
                    return (long)memStatus.ullAvailPhys;
                }
            }

            if (TryReadProcMemInfo(out _, out var available))
                return available;
            return 0;
#endif
        }

        private static long _cachedTotalMemory = 0;

        private static long GetTotalPhysicalMemory()
        {
            if (_cachedTotalMemory > 0)
                return _cachedTotalMemory;

            try
            {
#if NETFRAMEWORK
                // For .NET Framework, use Windows API via P/Invoke
                var memStatus = new MEMORYSTATUSEX();
                memStatus.dwLength = (uint)Marshal.SizeOf(typeof(MEMORYSTATUSEX));
                if (GlobalMemoryStatusEx(ref memStatus))
                {
                    _cachedTotalMemory = (long)memStatus.ullTotalPhys;
                    return _cachedTotalMemory;
                }
#else
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    var memStatus = new MEMORYSTATUSEX();
                    memStatus.dwLength = (uint)Marshal.SizeOf(typeof(MEMORYSTATUSEX));
                    if (GlobalMemoryStatusEx(ref memStatus))
                    {
                        _cachedTotalMemory = (long)memStatus.ullTotalPhys;
                        return _cachedTotalMemory;
                    }
                }
                else if (TryReadProcMemInfo(out var total, out _))
                {
                    _cachedTotalMemory = total;
                    return _cachedTotalMemory;
                }
#endif
            }
            catch
            {
                // Ignore and use fallback
            }

            // Fallback: assume 8GB if we can't determine
            _cachedTotalMemory = 8L * 1024 * 1024 * 1024;
            return _cachedTotalMemory;
        }

        internal static bool TryParseProcMemInfo(string procMemInfo, out long totalBytes, out long availableBytes)
        {
            totalBytes = 0;
            availableBytes = 0;
            if (string.IsNullOrEmpty(procMemInfo))
                return false;

            long memTotalKb = 0;
            long memAvailableKb = 0;
            long memFreeKb = 0;
            long buffersKb = 0;
            long cachedKb = 0;

            using (var reader = new StringReader(procMemInfo))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    var colon = line.IndexOf(':');
                    if (colon <= 0)
                        continue;
                    var key = line.Substring(0, colon);
                    var rest = line.Substring(colon + 1).Trim();
                    var space = rest.IndexOf(' ');
                    var number = space >= 0 ? rest.Substring(0, space) : rest;
                    if (!long.TryParse(number, NumberStyles.Integer, CultureInfo.InvariantCulture, out var kb))
                        continue;

                    if (key == "MemTotal")
                        memTotalKb = kb;
                    else if (key == "MemAvailable")
                        memAvailableKb = kb;
                    else if (key == "MemFree")
                        memFreeKb = kb;
                    else if (key == "Buffers")
                        buffersKb = kb;
                    else if (key == "Cached")
                        cachedKb = kb;
                }
            }

            if (memTotalKb <= 0)
                return false;

            if (memAvailableKb <= 0)
                memAvailableKb = memFreeKb + buffersKb + cachedKb;

            totalBytes = memTotalKb * 1024;
            availableBytes = Math.Max(0, memAvailableKb) * 1024;
            return true;
        }

        private static bool TryReadProcMemInfo(out long totalBytes, out long availableBytes)
        {
            totalBytes = 0;
            availableBytes = 0;
            try
            {
                return TryParseProcMemInfo(File.ReadAllText("/proc/meminfo"), out totalBytes, out availableBytes);
            }
            catch
            {
                return false;
            }
        }

        // P/Invoke declarations - shared between .NET Framework and .NET Core on Windows
        [StructLayout(LayoutKind.Sequential)]
        private struct MEMORYSTATUSEX
        {
            public uint dwLength;
            public uint dwMemoryLoad;
            public ulong ullTotalPhys;
            public ulong ullAvailPhys;
            public ulong ullTotalPageFile;
            public ulong ullAvailPageFile;
            public ulong ullTotalVirtual;
            public ulong ullAvailVirtual;
            public ulong ullAvailExtendedVirtual;
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool GlobalMemoryStatusEx(ref MEMORYSTATUSEX lpBuffer);

#if NETFRAMEWORK
        [StructLayout(LayoutKind.Sequential)]
        private struct FILETIME
        {
            public uint dwLowDateTime;
            public uint dwHighDateTime;
            public long ToInt64() => ((long)dwHighDateTime << 32) | dwLowDateTime;
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool GetSystemTimes(out FILETIME lpIdleTime, out FILETIME lpKernelTime, out FILETIME lpUserTime);
#else
        [DllImport("kernel32.dll", SetLastError = true, EntryPoint = "GetSystemTimes")]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool GetSystemTimesInternal(out long lpIdleTime, out long lpKernelTime, out long lpUserTime);

        private static bool GetSystemTimesCore(out long idleTime, out long kernelTime, out long userTime)
        {
            return GetSystemTimesInternal(out idleTime, out kernelTime, out userTime);
        }
#endif

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            Stop();
            _cts.Dispose();
        }
    }

    /// <summary>
    /// Represents a single metrics sample.
    /// </summary>
    internal class MetricSample
    {
        /// <summary>
        /// Timestamp in milliseconds since Unix epoch.
        /// </summary>
        public long TimestampMs { get; set; }

        /// <summary>
        /// CPU usage percentage (0-100).
        /// </summary>
        public double CpuPercent { get; set; }

        /// <summary>
        /// Memory usage percentage (0-100).
        /// </summary>
        public double MemoryPercent { get; set; }

        /// <summary>
        /// Network utilization percentage (0-100). Max of all interface TX/RX.
        /// </summary>
        public double NetPercent { get; set; }

        /// <summary>
        /// Per-interface network metrics (for tooltip details).
        /// </summary>
        public NetworkInterfaceMetric[] NetworkInterfaces { get; set; }
    }

    /// <summary>
    /// Network utilization metrics for a single interface.
    /// </summary>
    internal class NetworkInterfaceMetric
    {
        /// <summary>Interface name (truncated to 12 chars).</summary>
        public string Name { get; set; }

        /// <summary>Transmit utilization percentage (0-100).</summary>
        public double TxPercent { get; set; }

        /// <summary>Receive utilization percentage (0-100).</summary>
        public double RxPercent { get; set; }
    }

    /// <summary>
    /// Tracks state for a network interface between samples.
    /// </summary>
    internal class NetworkInterfaceInfo
    {
        public string InterfaceId { get; set; }
        public string Name { get; set; }
        public long SpeedBps { get; set; }
        public long LastBytesSent { get; set; }
        public long LastBytesReceived { get; set; }
        public DateTime LastSampleTime { get; set; }
    }
}
