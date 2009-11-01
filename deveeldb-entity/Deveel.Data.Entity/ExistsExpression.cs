using System;
using System.Text;

namespace Deveel.Data.Entity {
	internal class ExistsExpression : NegatableExpression {
		public Expression Argument;

		public ExistsExpression(Expression f) {
			Argument = f;
		}

		internal override void WriteTo(StringBuilder sql) {
			sql.Append(Negated ? "NOT " : "");
			sql.Append("EXISTS(");
			Argument.WriteTo(sql);
			sql.Append(")");
		}
	}
}