using System;
using System.Collections;

namespace Deveel.Data.Services {
	public interface IScope : IDisposable {
		IScope OpenScope(string name);

		void Register(ServiceRegistration registration);

		IRegistrationConfiguration<TService> Bind<TService>(); 

		bool Unregister(Type serviceType, object serviceKey);

		object Resolve(Type serviceType, object serviceKey);

		IEnumerable ResolveAll(Type serviceType);
	}
}
