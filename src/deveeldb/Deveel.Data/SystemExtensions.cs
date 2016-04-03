using System;

using Deveel.Data.Diagnostics;

namespace Deveel.Data {
	public static class SystemExtensions {
		public static IEventSource AsEventSource(this ISystem system) {
			if (system == null)
				throw new ArgumentNullException("system");

			var source = system as IEventSource;
			if (source != null)
				return source;

			return new EventSource(system.Context, null);
		}
	}
}
