using System;

namespace Deveel.Data.Diagnostics {
	public static class CounterRegistryExtensions {
		public static bool TryCount<T>(this ICounterRegistry registry, string name, out T value) {
			Counter counter;
			if (!registry.TryCount(name, out counter)) {
				value = default(T);
				return false;
			}

			value = counter.ValueAs<T>();
			return true;
		}

		public static T GetCount<T>(this ICounterRegistry registry, string name) {
			Counter counter;
			if (!registry.TryCount(name, out counter))
				return default(T);

			return counter.ValueAs<T>();
		}
	}
}
