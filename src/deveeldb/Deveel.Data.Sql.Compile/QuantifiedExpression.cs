using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Compile {
	// Shortcut for the parser
	class QuantifiedExpression : SqlExpression {
		public bool IsAll { get; set; }

		public bool IsAny { get; set; }

		public SqlExpression Argument { get; set; }

		public override SqlExpressionType ExpressionType {
			get { return new SqlExpressionType(); }
		}
	}
}
