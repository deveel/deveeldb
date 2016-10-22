using System;

namespace Deveel.Data.Build {
	public interface IServiceUseWithBindingConfiguration<TService, TImplementation> where TImplementation : class, TService {
		IServiceUseWithBindingConfiguration<TService, TImplementation> HavingKey(object key);

		IServiceUseWithBindingConfiguration<TService, TImplementation> InScope(string scope);

		IServiceUseWithBindingConfiguration<TService, TImplementation> Replace(bool replace = true);
	}
}
