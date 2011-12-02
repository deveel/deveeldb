using System;
using System.Text;

namespace Deveel.Data.Entity {
	internal class LikeExpression : NegatableExpression {
		public Expression Argument;
		public Expression Pattern;

		internal override void WriteTo(StringBuilder sql) {
			Argument.WriteTo(sql);
			if (Negated)
				sql.Append(" NOT ");
			sql.Append(" LIKE ");
			Pattern.WriteTo(sql);
		}
	}
}