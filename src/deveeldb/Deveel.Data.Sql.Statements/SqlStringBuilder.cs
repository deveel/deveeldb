using System;
using System.Text;

namespace Deveel.Data.Sql.Statements {
	public sealed class SqlStringBuilder {
		private readonly StringBuilder builder;

		internal SqlStringBuilder() {
			builder = new StringBuilder();
		}

		private int IndentCount { get; set; }

		public void Indent() {
			IndentCount++;
		}

		public void DeIndent() {
			var count = IndentCount--;
			if (count <= 0)
				count = 0;

			IndentCount = count;
		}

		public void Append(string format, params object[] args) {
			if (String.IsNullOrEmpty(format))
				throw new ArgumentNullException("format");

			Append(String.Format(format, args));
		}

		public void Append(string s) {
			if (String.IsNullOrEmpty(s))
				throw new ArgumentNullException("s");

			for (int i = 0; i < IndentCount; i++) {
				builder.Append(" ");
			}

			builder.Append(s);
		}

		public void Append(object obj) {
			if (obj == null)
				throw new ArgumentNullException("obj");

			Append(obj.ToString());
		}

		public void AppendLine(string s) {
			Append(s);
			AppendLine();
		}

		public void AppendLine() {
			Append('\n');
		}

		public override string ToString() {
			return builder.ToString();
		}
	}
}
