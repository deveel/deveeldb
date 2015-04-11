using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Diagnostics {
	public interface IEventRouter : IDatabaseService {
		void RouteEvent(IEvent e);
	}
}
