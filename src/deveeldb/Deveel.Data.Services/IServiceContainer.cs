using System;
using System.Collections;

namespace Deveel.Data.Services {
	public interface IServiceContainer : IDisposable {
		object Resolve(Type serviceType, string serviceName);

		IEnumerable ResolveAll(Type serviceType);

		void Register(Type serviceType, string name, object instance);

		void Unregister(Type serviceType, string name);
	}
}
