using System;

using Deveel.Data.Services;

namespace Deveel.Data.Diagnostics {
	public interface IEventScope : IContext {
		IEventRegistry EventRegistry { get; }
	}
}
