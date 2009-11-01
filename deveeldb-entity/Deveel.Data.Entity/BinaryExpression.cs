using System;
using System.Text;

namespace Deveel.Data.Entity {
	internal class BinaryExpression : NegatableExpression {
		public Expression Left { get; set;}
		public Expression Right { get; set; }
		public string Operator { get; set; }
		public bool WrapLeft { get; set; }
		public bool WrapRight { get; set; }

		internal override void WriteTo(StringBuilder sb) {
			if (Negated)
				sb.Append("NOT ");

			// do left arg
			if (WrapLeft)
				sb.Append("(");
			Left.WriteTo(sb);
			if (WrapLeft)
				sb.Append(")");

			sb.Append(' ');
			sb.Append(Operator);
			sb.Append(' ');

			// now right arg
			if (WrapRight)
				sb.Append("(");
			Right.WriteTo(sb);
			if (WrapRight)
				sb.Append(")");
		}
	}
}