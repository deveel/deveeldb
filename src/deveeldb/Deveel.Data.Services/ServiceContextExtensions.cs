using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Services {
	public static class ServiceContextExtensions {
		public static void RegisterService(this IServiceContext context, Type serviceType) {
			RegisterService(context, serviceType, null);
		}

		public static void RegisterService(this IServiceContext context, Type serviceType, string serviceName) {
			RegisterService(context, serviceType, serviceName, null);
		}

		public static void RegisterService(this IServiceContext context, Type serviceType, object instance) {
			RegisterService(context, serviceType, null, instance);
		}

		public static void RegisterService(this IServiceContext context, Type serviceType, string serviceName, object instance) {
			context.Container.Register(serviceType, serviceName, instance);
		}

		public static void RegisterService<TService>(this IServiceContext context)
			where TService : class {
			RegisterService<TService>(context, (string)null);
		}

		public static void RegisterService<TService>(this IServiceContext context, string serviceName)
			where TService : class {
			RegisterService<TService>(context, serviceName, null);
		}

		public static void RegisterService<TService>(this IServiceContext context, TService service)
			where TService : class {
			RegisterService<TService>(context, null, service);
		}

		public static void RegisterService<TService>(this IServiceContext context, string serviceName, TService service)
			where TService : class {
			context.Container.Register<TService>(serviceName, service);
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
