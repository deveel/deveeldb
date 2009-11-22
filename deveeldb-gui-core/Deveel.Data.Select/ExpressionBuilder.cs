using System;
using System.Text;

namespace Deveel.Data.Select {
	sealed class ExpressionBuilder {
		public ExpressionBuilder() {
			builder = new StringBuilder();
		}

		private readonly StringBuilder builder;

		public ExpressionBuilder Append(string token) {
			builder.Append(" ");
			builder.Append(token);
			builder.Append(" ");
			return this;
		}

		public override string ToString() {
			return builder.ToString();
		}
	}
}