using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Services {
	public static class ServiceContainerExtensions {
		public static void Register<TService>(this IServiceContainer container)
			where TService : class {
			Register<TService>(container, null);
		}

		public static void Register<TService>(this IServiceContainer container, string name)
			where TService : class {
			Register<TService>(container, name, null);
		}

		public static void Register<TService>(this IServiceContainer container, object service)
			where TService : class {
			Register<TService>(container, null, service);
		}

		public static void Register<TService>(this IServiceContainer container, string name, object service)
			where TService : class {
			container.Register(typeof(TService), name, service);
		}

		public static void Register(this IServiceContainer container, Type serviceType, string name) {
			container.Register(serviceType, name, null);
		}

		public static void Register(this IServiceContainer container, Type serviceType, object instance) {
			container.Register(serviceType, null, instance);
		}

		public static void Register(this IServiceContainer container, Type serviceType) {
			container.Register(serviceType, null, null);
		}

		public static TService Resolve<TService>(this IServiceContainer container) where TService : class {
			return Resolve<TService>(container, null);
		}

		public static TService Resolve<TService>(this IServiceContainer container, string name) where TService : class {
			return container.Resolve(typeof (TService), name) as TService;
		}

		public static IEnumerable<TService> ResolveAll<TService>(this IServiceContainer container) where TService : class {
			return container.ResolveAll(typeof (TService)).Cast<TService>();
		}
	}
}
