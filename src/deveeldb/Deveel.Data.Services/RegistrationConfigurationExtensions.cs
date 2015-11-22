using System;

namespace Deveel.Data.Services {
	public static class RegistrationConfigurationExtensions {
		public static IRegistrationWithBindingConfiguration<TService, TImplementation> InSystemScope
			<TService, TImplementation>(this IRegistrationWithBindingConfiguration<TService, TImplementation> configuration)
			where TImplementation : class, TService {
			return configuration.InScope(ContextNames.System);
		}

		public static IRegistrationWithBindingConfiguration<TService, TImplementation> InDatabaseScope
			<TService, TImplementation>(this IRegistrationWithBindingConfiguration<TService, TImplementation> configuration)
			where TImplementation : class, TService {
			return configuration.InScope(ContextNames.Database);
		}

		public static IRegistrationWithBindingConfiguration<TService, TImplementation> InQueryScope
			<TService, TImplementation>(this IRegistrationWithBindingConfiguration<TService, TImplementation> configuration)
			where TImplementation : class, TService {
			return configuration.InScope(ContextNames.Query);
		}

		public static IRegistrationWithBindingConfiguration<TService, TImplementation> InSessionScope
			<TService, TImplementation>(this IRegistrationWithBindingConfiguration<TService, TImplementation> configuration)
			where TImplementation : class, TService {
			return configuration.InScope(ContextNames.Session);
		}

		public static IRegistrationWithBindingConfiguration<TService, TImplementation> InTransactionScope
			<TService, TImplementation>(this IRegistrationWithBindingConfiguration<TService, TImplementation> configuration)
			where TImplementation : class, TService {
			return configuration.InScope(ContextNames.Transaction);
		}

		public static IRegistrationWithBindingConfiguration<TService, TImplementation> InBlockScope
			<TService, TImplementation>(this IRegistrationWithBindingConfiguration<TService, TImplementation> configuration)
			where TImplementation : class, TService {
			return configuration.InScope(ContextNames.Block);
		}

		public static IRegistrationWithBindingConfiguration<TService, TService> ToSelf<TService>(this IRegistrationConfiguration<TService> configuration)
			where TService : class {
			return configuration.To<TService>();
		}
	}
}
