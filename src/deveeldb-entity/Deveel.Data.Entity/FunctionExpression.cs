using System;
using System.Text;

namespace Deveel.Data.Entity {
	internal class FunctionExpression : Expression {
		public bool Distinct;
		public Expression Argmument;
		public string Name;

		internal override void WriteTo(StringBuilder sb) {
			sb.Append(Name);
			sb.Append("(");
			if (Distinct)
				sb.Append("DISTINCT ");
			Argmument.WriteTo(sb);
			sb.Append(")");
		}
	}
}