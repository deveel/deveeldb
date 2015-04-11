using System;
using System.Collections;

namespace Deveel.Data.DbSystem {
	public interface ISystemServiceProvider : IServiceProvider, IDisposable {
		object Resolve(Type serviceType, string name);

		IEnumerable ResolveAll(Type serviceType);
	}
}
