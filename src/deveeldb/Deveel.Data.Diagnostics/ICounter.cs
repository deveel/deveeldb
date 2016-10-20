using System;

namespace Deveel.Data.Diagnostics {
	public interface ICounter {
		string Name { get; }

		object Value { get; }
	}
}
