using System;
using System.Collections;

namespace Deveel.Data.Services {
	public interface IResolveScope {
		object OnBeforeResolve(Type serviceType, string name);

		void OnAfterResolve(Type serviceType, string name, object obj);

		IEnumerable OnBeforeResolveAll(Type serviceType);

		void OnAfterResolveAll(Type serviceType, IEnumerable list);
	}
}
