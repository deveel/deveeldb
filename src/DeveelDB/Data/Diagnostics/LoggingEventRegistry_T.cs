using System;

using Deveel.Data.Events;

namespace Deveel.Data.Diagnostics {
	public abstract class LoggingEventRegistry<TEvent> : LoggingEventRegistry, IEventRegistry<TEvent>
		where TEvent : class, IEvent {
		protected LoggingEventRegistry(ILogger logger) : base(logger) {
		}

		protected LoggingEventRegistry(ILogger logger, IEventTransformer transformer) : base(logger, transformer) {
		}

		protected override Type EventType => typeof(TEvent);

		void IEventRegistry<TEvent>.Register(TEvent @event) {
			(this as IEventRegistry).Register(@event);
		}
	}
}