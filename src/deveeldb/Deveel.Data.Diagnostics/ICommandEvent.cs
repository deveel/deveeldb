using System;
using System.Collections.Generic;

namespace Deveel.Data.Diagnostics {
	public interface ICommandEvent : IEvent {
		string CommandType { get; }

		IEnumerable<KeyValuePair<string, object>> Arguments { get; }
	}
}
