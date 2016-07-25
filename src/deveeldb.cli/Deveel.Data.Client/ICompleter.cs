using System;
using System.Collections.Generic;

namespace Deveel.Data.Client {
	public interface ICompleter {
		IEnumerable<string> Complete(string[] tokens, string currentToken);

		bool IsComplete(string text);
	}
}
