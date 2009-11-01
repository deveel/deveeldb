using System;
using System.Text;

namespace Deveel.Data.Entity {
	internal class CompositeExpression : BranchExpression {
		public string Function { get; set; }
		public bool All { get; set; }

		internal override void WriteTo(StringBuilder sb) {
			Left.WriteTo(sb);
			sb.Append(' ');
			sb.Append(Function);
			if (All) {
				sb.Append(' ');
				sb.Append("ALL");
				sb.Append(' ');
			}
			sb.Append(' ');
			Right.WriteTo(sb);
		}
	}
}