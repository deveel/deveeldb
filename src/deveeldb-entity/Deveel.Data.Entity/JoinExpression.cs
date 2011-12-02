using System;
using System.Text;

namespace Deveel.Data.Entity {
	internal class JoinExpression : BranchExpression {
		public Expression Condition { get; set; }
		public string JoinType { get; set; }

		protected override void InnerWrite(StringBuilder sb) {
			Left.WriteTo(sb);
			sb.Append(' ');
			sb.Append(JoinType);
			sb.Append(' ');
			Right.WriteTo(sb);

			if (Condition != null) {
				sb.Append(' ');
				sb.Append("ON");
				sb.Append(' ');
				Condition.WriteTo(sb);
			}
		}
	}
}