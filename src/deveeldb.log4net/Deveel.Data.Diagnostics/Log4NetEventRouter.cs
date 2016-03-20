using System;

using log4net;
using log4net.Config;

namespace Deveel.Data.Diagnostics {
	public class Log4NetEventRouter : IEventRouter {
		protected Log4NetEventRouter(ILog logger) {
			if (logger == null)
				throw new ArgumentNullException("logger");

			Logger = logger;
		}

		protected ILog Logger { get; private set; }

		public bool CanRoute(IEvent @event) {
			if (@event is ErrorEvent) {
				var error = (ErrorEvent) @event;
				if (error.Level == ErrorLevel.Error)
					return Logger.IsErrorEnabled;
				if (error.Level == ErrorLevel.Critical)
					return Logger.IsFatalEnabled;
				if (error.Level == ErrorLevel.Warning)
					return Logger.IsWarnEnabled;
			} else if (@event is InformationEvent) {
				var info = (InformationEvent) @event;
				if (info.Level == InformationLevel.Debug)
					return Logger.IsDebugEnabled;
				if (info.Level == InformationLevel.Information ||
				    info.Level == InformationLevel.Verbose)
					return Logger.IsInfoEnabled;
			}

			return false;
		}

		public void RouteEvent(IEvent e) {
			var message = new LogMessage {
				Database = e.DatabaseName(),
				User = e.UserName(),
				TimeStamp = e.TimeStamp
			};

			if (e is ErrorEvent) {
				var error = (ErrorEvent) e;

				message.ErrorCode = error.ErrorCode;
				message.Text = error.Error.Message;

				if (error.Level == ErrorLevel.Error) {
					Logger.Error(message, error.Error);
				} else if (error.Level == ErrorLevel.Critical) {
					Logger.Fatal(message, error.Error);
				} else if (error.Level == ErrorLevel.Warning) {
					Logger.Warn(message, error.Error);
				}
			} else if (e is InformationEvent) {
				var info = (InformationEvent) e;

				message.Text = info.Message;

				if (info.Level == InformationLevel.Debug) {
					Logger.Debug(message);
				} else if (info.Level == InformationLevel.Information ||
				           info.Level == InformationLevel.Verbose) {
					Logger.Info(message);
				}
			}
		}

		internal static void Setup() {
			XmlConfigurator.Configure();

			// TODO: Add the patterns
		}
	}
}
