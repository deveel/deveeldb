using System;

namespace Deveel.Data.Diagnostics {
	public interface IDatabaseEventSource {
		string DatabaseName { get; }
	}
}
