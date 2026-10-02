using System;
using System.IO;
using NUnit.Framework;
using TestRift.NUnit;

namespace TestRift.NUnit
{
    [SetUpFixture]
    public class RunHooks
    {
        public RunHooks() : this("")
        {
        }

        public RunHooks(string configPath)
        {
            Console.WriteLine("=== RunHooks CONSTRUCTOR CALLED ===");
            ThreadSafeFileLogger.LogRunHooksConstructor();

            // Use constructor parameter first, then environment variable, then ./TestRiftNUnit.yaml in CWD.
            var finalConfigPath = string.IsNullOrEmpty(configPath) ? null : configPath;
            if (string.IsNullOrEmpty(finalConfigPath))
            {
                finalConfigPath = Environment.GetEnvironmentVariable("TESTRIFT_NUNIT_YAML");
            }

            if (string.IsNullOrEmpty(finalConfigPath))
            {
                var cwdDefault = Path.Combine(Directory.GetCurrentDirectory(), "TestRiftNUnit.yaml");
                if (File.Exists(cwdDefault))
                {
                    finalConfigPath = cwdDefault;
                }
            }

            if (!string.IsNullOrEmpty(finalConfigPath))
            {
                try
                {
                    ConfigManager.Load(finalConfigPath);
                    Console.WriteLine($"Loaded TestRift NUnit config: {finalConfigPath}");

                    // Optional: auto-start TestRift Server for local dev/CI convenience.
                    var cfg = ConfigManager.Get();
                    var auto = cfg.AutoStartServer;
                    if (auto != null && auto.Enabled)
                    {
                        ServerAutoStarter.EnsureServerRunning(cfg.ServerUrl, auto.ServerYaml, auto.RestartOnConfigChange);
                    }
                }
                catch (Exception ex)
                {
                    // Reporting problems must not prevent the tests themselves from running.
                    TRLoggerAttribute.DisableReporting($"cannot load {finalConfigPath}: {ex.Message}");
                }
            }
        }

        [OneTimeSetUp]
        public void RunStarted()
        {
            Console.WriteLine("=== TEST RUN STARTED ===");
            ThreadSafeFileLogger.LogRunHooksStarted();
            // Tell TRLogger that test run has started
            TRLoggerAttribute.NotifyRunStarted();
        }

        [OneTimeTearDown]
        public void RunFinished()
        {
            Console.WriteLine("=== TEST RUN FINISHED ===");
            ThreadSafeFileLogger.LogRunHooksFinished();
            // Tell TRLogger that test run has finished
            TRLoggerAttribute.NotifyRunFinished();
        }
    }
}
