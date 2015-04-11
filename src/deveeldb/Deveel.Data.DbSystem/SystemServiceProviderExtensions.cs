using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.DbSystem {
	public static class SystemServiceProviderExtensions {
		public static object Resolve(this ISystemServiceProvider provider, Type type) {
			return provider.Resolve(type, null);
		}

		public static T Resolve<T>(this ISystemServiceProvider provider) {
			return Resolve<T>(provider, null);
		}

		public static T Resolve<T>(this ISystemServiceProvider provider, string name) {
			return (T) provider.Resolve(typeof (T), name);
		}

		public static IEnumerable<T> ResolveAll<T>(this ISystemServiceProvider provider) {
			return provider.ResolveAll(typeof (T)).Cast<T>();
		}
	}
}
