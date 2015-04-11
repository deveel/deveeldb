using System;
using System.Collections;

namespace Deveel.Data.DbSystem {
	public interface IServiceResolveContext {
		object OnResolve(Type type, string name);

		void OnResolved(Type type, string name, object obj);

		IEnumerable OnResolveAll(Type type);

		void OnResolvedAll(Type type, IEnumerable list);
	}
}
