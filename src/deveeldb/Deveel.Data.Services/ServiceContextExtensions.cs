using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Services {
	public static class ServiceContextExtensions {
		public static void RegisterService(this IServiceContext context, Type serviceType) {
			context.Container.Register(serviceType);
		}

		public static void RegisterService(this IServiceContext context, Type serviceType, string serviceName) {
			context.Container.Register(serviceType, serviceName);
		}

		public static void RegisterService(this IServiceContext context, Type serviceType, object instance) {
			context.Container.RegisterInstance(serviceType, instance);
		}

		public static void RegisterService(this IServiceContext context, Type serviceType, string serviceName, object instance) {
			context.Container.RegisterInstance(serviceType, instance, serviceName);
		}

		public static void RegisterService<TService>(this IServiceContext context)
			where TService : class {
			context.Container.Register<TService>();
		}

		public static void RegisterService<TService>(this IServiceContext context, string serviceName)
			where TService : class {
			context.Container.Register<TService>(serviceName);
		}

		public static void RegisterService<TService, TImplementation>(this IServiceContext context, string serviceName)
			where TImplementation : class, TService {
			context.Container.Register<TService, TImplementation>(serviceName);
		}

		public static void RegisterService<TService>(this IServiceContext context, TService service)
			where TService : class {
			context.Container.RegisterInstance(service);
		}

		public static void RegisterService<TService>(this IServiceContext context, TService service, string serviceName)
			where TService : class {
			context.Container.RegisterInstance(service, serviceName);
		}

		public static void UnregisterService(this IServiceContext context, Type serviceType) {
			UnregisterService(context, serviceType, null);
		}

		public static void UnregisterService(this IServiceContext context, Type serviceType, string serviceName) {
			context.Container.Unregister(serviceType, serviceName);
		}

		public static void UnregisterService<TService>(this IServiceContext context)
			where TService : class {
			UnregisterService<TService>(context, null);
		}

		public static void UnregisterService<TService>(this IServiceContext context, string serviceName)
			where TService : class {
			context.Container.Unregister(typeof(TService), serviceName);
		}

		public static object ResolveService(this IServiceContext context, Type serviceType) {
			return ResolveService(context, serviceType, null);
		}

		public static object ResolveService(this IServiceContext context, Type serviceType, string serviceName) {
			return context.Container.Resolve(serviceType, serviceName);
		}

		public static TService ResolveService<TService>(this IServiceContext context)
			where TService : class {
			return ResolveService<TService>(context, null);
		}

		public static TService ResolveService<TService>(this IServiceContext context, string serviceName)
			where TService : class {
			return context.Container.Resolve<TService>(serviceName);
		}

		public static IEnumerable<TService> ResolveAllServices<TService>(this IServiceContext context) where TService : class {
			return context.Container.ResolveAll<TService>();
		}

		public static IEnumerable ResolveAllServices(this IServiceContext context, Type serviceType) {
			return context.Container.ResolveAll(serviceType);
		}
	}
}
