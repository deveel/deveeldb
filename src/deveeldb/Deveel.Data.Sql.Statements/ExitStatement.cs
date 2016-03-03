using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class ExitStatement : LoopControlStatement {
		public ExitStatement()
			: this((string)null) {
		}

		public ExitStatement(string label)
			: this(label, null) {
		}

		public ExitStatement(SqlExpression whenExpression)
			: this(null, whenExpression) {
		}

		public ExitStatement(string label, SqlExpression whenExpression)
			: base(LoopControlType.Exit, label, whenExpression) {
		}
	}
}
