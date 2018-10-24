using System;

using Deveel.Data.Events;

namespace Deveel.Data.Diagnostics {
	public abstract class LoggingEventRegistry : IEventRegistry {
		protected LoggingEventRegistry(ILogger logger) : this(logger, null) {
		}

		protected LoggingEventRegistry(ILogger logger, IEventTransformer transformer) {
			Logger = logger ?? throw new ArgumentNullException(nameof(logger));
			Transformer = transformer;
		}

		Type IEventRegistry.EventType => EventType;

		protected ILogger Logger { get; }

		protected abstract Type EventType { get; }

		protected IEventTransformer Transformer { get; }

		protected virtual LogEntry OnEvent(IEvent @event) {
			return null;
		}

		public void Register(IEvent @event) {
			if (!EventType.IsInstanceOfType(@event))
				throw new LogException($"The event is not compatible with the handled type '{EventType}'.");

			LogEvent(@event);
		}

		private void LogEvent(IEvent @event) {
			try {
				var entry = Transformer?.Transform(@event);
				if (entry == null)
					entry = OnEvent(@event);
				if (entry == null)
					throw new LogException("It was not possible to generate a log entry from an event");

				if (Logger.IsInterestedIn(entry.Level))
					Logger.LogAsync(entry).ConfigureAwait(false).GetAwaiter().GetResult();
			} catch(LogException) {
				throw;
			} catch (Exception ex) {
				throw new LogException("An error occurred while logging an event", ex);
			}
		}
	}
}