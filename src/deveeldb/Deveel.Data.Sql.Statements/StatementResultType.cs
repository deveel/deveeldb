using System;

namespace Deveel.Data.Sql.Statements {
	public enum StatementResultType {
		Empty = 0,
		CursorRef = 1,
		Result = 2,
		Exception = 3
	}
}
