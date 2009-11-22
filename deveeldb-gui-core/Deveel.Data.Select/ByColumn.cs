using System;
using System.Text;

namespace Deveel.Data.Select {
	public sealed class ByColumn {
		private readonly string expression;
		private readonly bool ascending;

		public ByColumn(string expression, bool ascending) {
			this.expression = expression;
			this.ascending = ascending;
		}

		public ByColumn(string name)
			: this(name, true) {
		}

		public bool Ascending {
			get { return ascending; }
		}

		public string Expression {
			get { return expression; }
		}

		public override string ToString() {
			StringBuilder sb = new StringBuilder();
			DumpTo(sb);
			return sb.ToString();
		}

		internal void DumpTo(StringBuilder sb) {
			sb.Append(expression);
			sb.Append(" ");
			sb.Append(ascending ? "ASC" : "DESC");
		}
	}
}