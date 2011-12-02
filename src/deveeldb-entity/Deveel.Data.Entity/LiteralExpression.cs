using System;
using System.Text;

namespace Deveel.Data.Entity {
	internal class LiteralExpression : Expression {
		public LiteralExpression() {
			
		}

		public LiteralExpression(string text) {
			Text = text;
		}

		public string Text { get; set; }

		internal override void WriteTo(StringBuilder sb) {
			sb.Append(Text);
		}
	}
}