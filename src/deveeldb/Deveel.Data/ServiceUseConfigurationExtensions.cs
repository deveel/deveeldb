using System;
using System.Runtime.Remoting.Services;

namespace Deveel.Data {
	public static class ServiceUseConfigurationExtensions {
		public static IServiceUseWithBindingConfiguration<TService, TService> ToSelf<TService>(this
			IServiceUseConfiguration<TService> configuration)
			where TService : class {
			return configuration.To<TService>();
		}

		public static IServiceUseWithBindingConfiguration<TService, TImplementation> InSystemScope<TService, TImplementation>(
			this IServiceUseWithBindingConfiguration<TService, TImplementation> configuration)
			where TImplementation : class, TService {
			return configuration.InScope(ContextNames.System);
		}

		public static IServiceUseWithBindingConfiguration<TService, TImplementation> InTransactionScope
			<TService, TImplementation>(
				this IServiceUseWithBindingConfiguration<TService, TImplementation> configuration)
			where TImplementation : class, TService {
			return configuration.InScope(ContextNames.Transaction);
		}

		public static IServiceUseWithBindingConfiguration<TService, TImplementation> InDatabaseScope
			<TService, TImplementation>(
				this IServiceUseWithBindingConfiguration<TService, TImplementation> configuration)
			where TImplementation : class, TService {
			return configuration.InScope(ContextNames.Database);
		}

		public static IServiceUseWithBindingConfiguration<TService, TImplementation> InQueryScope<TService, TImplementation>(
			this IServiceUseWithBindingConfiguration<TService, TImplementation> configuration)
			where TImplementation : class, TService {
			return configuration.InScope(ContextNames.Query);
		}

		public static IServiceUseWithBindingConfiguration<TService, TImplementation> InSessionScope<TService, TImplementation>(
	this IServiceUseWithBindingConfiguration<TService, TImplementation> configuration)
	where TImplementation : class, TService {
			return configuration.InScope(ContextNames.Session);
		}
	}
}