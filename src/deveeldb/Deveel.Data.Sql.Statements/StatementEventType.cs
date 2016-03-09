using System;

namespace Deveel.Data.Sql.Statements {
	public enum StatementEventType {
		BeforePrepare = 1,
		AfterPrepare = 2,
		BeforeExecute = 3,
		AfterExecute = 4
	}
}
