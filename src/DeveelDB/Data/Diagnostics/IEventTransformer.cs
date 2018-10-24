using System;

using Deveel.Data.Events;

namespace Deveel.Data.Diagnostics {
	public interface IEventTransformer {
		LogEntry Transform(IEvent @event);
	}
}