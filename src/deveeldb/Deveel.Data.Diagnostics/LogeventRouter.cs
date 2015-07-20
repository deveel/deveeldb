using System;

using Deveel.Data.Configuration;
using Deveel.Data.DbSystem;

namespace Deveel.Data.Diagnostics {
	/// <summary>
	/// An event router that listens to events (errors or notifications)
	/// and routes to <see cref="IEventLogger"/> implementations configured
	/// within the system.
	/// </summary>
	public sealed class LogEventRouter : IEventRouter {
		/// <summary>
		/// Constructs the router around the given system context.
		/// </summary>
		/// <param name="context">The context of the system where this
		/// router relies on.</param>
		/// <exception cref="ArgumentNullException">
		/// If the <paramref name="context"/> parameter is <c>null</c>.
		/// </exception>
		public LogEventRouter(ISystemContext context) {
			if (context == null)
				throw new ArgumentNullException("context");

			Context = context;
		}

		/// <summary>
		/// Gets the context of the system where this router belongs.
		/// </summary>
		public ISystemContext Context { get; private set; }

		void IConfigurable.Configure(IDbConfig config) {
		}

		public void RouteEvent(IEvent e) {
			var loggers = Context.ServiceProvider.ResolveAll<IEventLogger>();

			EventLog entry = null;

			if (e is NotificationEvent) {
				entry = CreateEntry((NotificationEvent) e);
			} else if (e is ErrorEvent) {
				entry = CreateEntry((ErrorEvent) e);
			}

			if (entry == null)
				return;

			foreach (var logger in loggers) {
				if (logger.CanLog(entry.Level))
					logger.LogEvent(entry);
			}
		}

		private EventLog CreateEntry(NotificationEvent @event) {
			return new EventLog(
				@event.EventClass,
				@event.EventCode,
				@event.ErrorSource(),
				GetLogLevel(@event.Level),
				@event.Database(),
				@event.UserName(),
				@event.RemoteAddress(),
				@event.EventMessage,
				null);
		}

		private LogLevel GetLogLevel(NotificationLevel level) {
			if (level == NotificationLevel.Debug ||
				level == NotificationLevel.Message)
				return LogLevel.Information;
			if (level == NotificationLevel.Verbose)
				return LogLevel.Verbose;
			
			return LogLevel.Undefined;
		}

		private LogLevel GetLogLevel(ErrorLevel level) {
			if (level == ErrorLevel.Critical)
				return LogLevel.Critical;
			if (level == ErrorLevel.Error)
				return LogLevel.Error;
			if (level == ErrorLevel.Warning)
				return LogLevel.Warning;

			return LogLevel.Undefined;
		}

		private EventLog CreateEntry(ErrorEvent @event) {
			return new EventLog(
				@event.EventClass,
				@event.ErrorCode,
				@event.ErrorSource(),
				GetLogLevel(@event.ErrorLevel()),
				@event.Database(),
				@event.UserName(),
				@event.RemoteAddress(),
				@event.Message,
				null);
		}
	}
}
