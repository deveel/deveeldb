using System;

namespace Deveel.Data.Services {
	public interface IRegistrationConfiguration<TService> {
		IRegistrationWithBindingConfiguration<TService, TImplementation> To<TImplementation>()
			where TImplementation : class, TService;

		IRegistrationWithBindingConfiguration<TService, TImplementation> ToInstance<TImplementation>(TImplementation instance)
			where TImplementation : class, TService;
	}
}
