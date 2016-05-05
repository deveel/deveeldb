using System;
using System.Collections.Generic;

namespace Deveel.Data.Diagnostics {
	public interface ICounterRegistry : IEnumerable<Counter> {
		bool TryCount(string name, out Counter counter);
	}
}
