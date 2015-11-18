// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;

using Deveel.Data.Services;

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

		public void RouteEvent(IEvent e) {
			if (e == null)
				return;

			if (e.EventType != (byte)EventType.Error &&
				e.EventType != (byte)EventType.Notification)
				return;

			var loggers = Context.ResolveAllServices<IEventLogger>();

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
