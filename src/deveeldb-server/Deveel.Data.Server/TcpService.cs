using System;
using System.Diagnostics;
using System.ServiceProcess;

using Deveel.Data.Control;

namespace Deveel.Data.Server {
	public sealed class TcpService : ServiceBase {
		public TcpService() {
			ServiceName = "DeveelDB TCP Service";
			EventLog.Log = "Application";

			CanHandlePowerEvent = true;
			CanStop = true;
			CanShutdown = true;
			CanHandlePowerEvent = true;
			CanPauseAndContinue = true;
		}

		private TcpServerController serverController;

		private const string DEVEEL_DB_PATH = "DEVEEL_DB_PATH";

		protected override void OnStart(string[] args) {
			try {
				// Connect a TcpServerController to it.
				string dbPath = Environment.GetEnvironmentVariable(DEVEEL_DB_PATH, EnvironmentVariableTarget.Process) ??
				                Environment.CurrentDirectory;

				DbConfig config = new DbConfig();
				config.SetValue(ConfigKeys.BasePath, dbPath);
				serverController = new TcpServerController(DbController.Create(config));
				// And start the server
				serverController.Start();
			} catch(Exception e) {
				throw new ApplicationException(e.Message, e);
			}
		}

		protected override void OnStop() {
			try {
				serverController.Stop();
			} catch (Exception e) {
				EventLog.WriteEntry(e.Message, EventLogEntryType.Error);
				ExitCode = 1;
			}
		}

		protected override void OnPause() {
			try {
				serverController.Stop();
			} catch (Exception e) {
				throw new ApplicationException(e.Message);
			}
		}

		protected override void OnContinue() {
			try {
				serverController.Start();
			} catch (Exception e) {
				throw new ApplicationException(e.Message);
			}
		}

		protected override void OnShutdown() {
			//TODO: request additional time until we stop cleanly...
			try {
				serverController.Stop();
				ExitCode = 0;
			} catch(Exception e) {
				EventLog.WriteEntry(e.Message, EventLogEntryType.Error);
				ExitCode = 1;
			}
		}
	}
}