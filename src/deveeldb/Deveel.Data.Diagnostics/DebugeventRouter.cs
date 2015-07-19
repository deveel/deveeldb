using System;

using Deveel.Data.Configuration;

namespace Deveel.Data.Diagnostics {
	public class DebugEventRouter : IEventRouter {
		public void Configure(IDbConfig config) {
			throw new NotImplementedException();
		}

		public void RouteEvent(IEvent e) {
			throw new NotImplementedException();
		}
	}
}
