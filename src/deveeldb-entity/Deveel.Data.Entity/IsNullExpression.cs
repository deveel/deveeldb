using System;
using System.Text;

namespace Deveel.Data.Entity {
	internal class IsNullExpression : NegatableExpression {
		public Expression Argument;

		internal override void WriteTo(StringBuilder sb) {
			Argument.WriteTo(sb);
			sb.Append(" IS ");
			if (Negated)
				sb.Append("NOT ");
			sb.Append("NULL");
		}
	}
}