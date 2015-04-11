using System;

namespace Deveel.Data.Diagnostics {
	public interface ISessionEventSource : IDatabaseEventSource {
		string UserName { get; }
	}
}
