using System;

using Deveel.Data.Services;

namespace Deveel.Data {
	public interface IContext : IDisposable {
		IContext Parent { get; }

		string Name { get; }

		IScope Scope { get; }
	}
}
