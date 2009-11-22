using System;
using System.Text;

namespace Deveel.Data.Select {
	public sealed class SelectColumn {
		public SelectColumn(string expression, string alias) {
			if (expression == null)
				throw new ArgumentNullException("expression");

			this.expression = expression;
			this.alias = alias;
		}

		public SelectColumn(string expression)
			: this(expression, null) {
		}

		internal SelectColumn() {
		}

		private string alias;
		private string expression;

		public string Alias {
			get { return alias; }
		}

		public string Expression {
			get { return expression; }
		}

		public static SelectColumn Identity {
			get { return new SelectColumn("IDENTITY"); }
		}

		internal void SetExpression(string exp) {
			expression = exp;
		}

		internal void SetAlias(string name) {
			alias = name;
		}

		public static SelectColumn Glob(string glob) {
			return new SelectColumn(glob);
		}

		public override string ToString() {
			StringBuilder sb = new StringBuilder();
			DumpTo(sb);
			return sb.ToString();
		}

		internal void DumpTo(StringBuilder sb) {
			sb.Append(expression);
			if (alias != null && alias.Length > 0) {
				sb.Append(" AS ");
				sb.Append(alias);
			}
		}
	}
}