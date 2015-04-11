using System;

namespace Deveel.Data.Diagnostics {
	public interface IEvent {
		string DatabaseName { get; }

		string UserName { get; }
	}
}
