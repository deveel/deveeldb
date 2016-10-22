using System;

namespace Deveel.Data.Build {
	public interface IServiceUseConfiguration<TService> {
		IServiceUseWithBindingConfiguration<TService, TImplementation> With<TImplementation>()
	where TImplementation : class, TService;

		IServiceUseWithBindingConfiguration<TService, TImplementation> With<TImplementation>(TImplementation instance)
			where TImplementation : class, TService;
	}
}
