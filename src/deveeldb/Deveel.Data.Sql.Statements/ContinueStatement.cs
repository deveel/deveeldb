using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class ContinueStatement : LoopControlStatement {
		public ContinueStatement()
			: this((string)null) {
		}

		public ContinueStatement(string label)
			: this(label, null) {
		}

		public ContinueStatement(SqlExpression whenExpression)
			: this(null, whenExpression) {
		}

		public ContinueStatement(string label, SqlExpression whenExpression)
			: base(LoopControlType.Continue, label, whenExpression) {
		}
	}
}
