using System;

namespace Deveel.Data.Services {
	public interface IContext : IDisposable {
		IContext Parent { get; }

		string Name { get; }

		IScope Scope { get; }
	}
}
