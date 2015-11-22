using System;

using Deveel.Data.Diagnostics;
using Deveel.Data.Services;

namespace Deveel.Data {
	public interface IOperation : IEventSource, IDisposable {
		IContext Context { get; }
	}
}
