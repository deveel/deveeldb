using System;

namespace Deveel.Data.Diagnostics {
	public interface ICountScope : IContext {
		ICounterRegistry Counters { get; }
	}
}
