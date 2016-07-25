using System;

namespace Deveel.Data.Client {
	public interface ICompleter {
		bool CanComplete(CompleteRequest request);

		CompleteResult Complete(CompleteRequest request);
	}
}
