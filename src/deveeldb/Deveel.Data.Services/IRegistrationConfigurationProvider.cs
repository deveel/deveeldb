using System;

namespace Deveel.Data.Services {
	interface IRegistrationConfigurationProvider {
		ServiceContainer Container { get; }

		Type ServiceType { get; }

		Type ImplementationType { get; }

		object ServiceKey { get; }

		string ScopeName { get; }

		object Instance { get; }
	}
}
