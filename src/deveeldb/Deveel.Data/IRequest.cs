using System;

using Deveel.Data.Diagnostics;

namespace Deveel.Data {
	public interface IRequest : IEventSource, IDisposable {
		IQuery Query { get; }
	}
}
