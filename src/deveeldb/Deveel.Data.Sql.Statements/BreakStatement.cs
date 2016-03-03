using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class BreakStatement : LoopControlStatement {
		public BreakStatement() 
			: this((string)null) {
		}

		public BreakStatement(string label) 
			: this(label, null) {
		}

		public BreakStatement(SqlExpression whenExpression) 
			: this(null, whenExpression) {
		}

		public BreakStatement(string label, SqlExpression whenExpression) 
			: base(LoopControlType.Break, label, whenExpression) {
		}
	}
}
