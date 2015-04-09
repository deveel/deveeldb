using System;

namespace Deveel.Data.Transactions {
	public interface ICallbackHandler {
		void OnCallbackAttached(TableCommitCallback callback);

		void OnCallbackDetached(TableCommitCallback callback);
	}
}
