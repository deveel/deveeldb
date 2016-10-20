using System;

namespace Deveel.Data {
	public interface IServiceUseConfiguration<TService> {
		IServiceUseWithBindingConfiguration<TService, TImplementation> To<TImplementation>()
	where TImplementation : class, TService;

		IServiceUseWithBindingConfiguration<TService, TImplementation> ToInstance<TImplementation>(TImplementation instance)
			where TImplementation : class, TService;
	}
}
