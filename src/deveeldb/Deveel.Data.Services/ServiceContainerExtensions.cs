using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Services {
	public static class ServiceContainerExtensions {
		public static void Register<TService>(this IServiceContainer container)
			where TService : class {
			Register(container, default(TService));
		}

		public static void Register<TService>(this IServiceContainer container, string name)
			where TService : class {
			Register<TService>(container, name, null);
		}

		public static void Register<TService>(this IServiceContainer container, TService service)
			where TService : class {
			Register<TService>(container, null, service);
		}

		public static void Register<TService>(this IServiceContainer container, string name, TService service)
			where TService : class {
			container.RegisterInstance<TService>(service, name);
		}

		public static void Register(this IServiceContainer container, Type serviceType, string name) {
			container.Register(serviceType, serviceType, name);
		}

		public static void Register(this IServiceContainer container, Type serviceType, object instance) {
			container.RegisterInstance(serviceType, instance);
		}

		public static void Register(this IServiceContainer container, Type serviceType) {
			container.Register(serviceType, serviceType);
		}

		public static void Unregister<TService>(this IServiceContainer container) 
			where TService : class {
			Unregister<TService>(container, null);
		}

		public static void Unregister<TService>(this IServiceContainer container, string name) 
			where TService : class {
			container.Unregister(typeof(TService), name);
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
