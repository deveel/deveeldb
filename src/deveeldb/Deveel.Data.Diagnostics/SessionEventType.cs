using System;

namespace Deveel.Data.Diagnostics {
	public enum SessionEventType {
		Begin = 1,
		EndForCommit = 2,
		EndForRollback = 3
		//TODO: Abandoned?
	}
}
