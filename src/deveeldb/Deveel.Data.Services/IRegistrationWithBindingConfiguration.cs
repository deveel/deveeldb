using System;

namespace Deveel.Data.Services {
	public interface IRegistrationWithBindingConfiguration<TService, TImplementation> {
		IRegistrationWithBindingConfiguration<TService, TImplementation> InScope(string scopeName);

		IRegistrationWithBindingConfiguration<TService, TImplementation> WithKey(object serviceKey);
	}
}
