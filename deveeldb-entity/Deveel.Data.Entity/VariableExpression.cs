using System;
using System.Text;

namespace Deveel.Data.Entity {
	internal class VariableExpression : Expression {
		public Expression Text { get; set; }
		public string TableName { get; set; }
		public string TableAlias { get; set; }
		public string ColumnName { get; set; }
		public string ColumnAlias { get; set; }

		internal override void WriteTo(StringBuilder sb) {
			if (Text != null) {
				Text.WriteTo(sb);
			} else {
				string tableName = TableName;
				if (TableAlias != null)
					tableName = TableAlias;

				sb.Append(Quote(tableName));
				sb.Append('.');
				sb.Append(Quote(ColumnName));
				if (ColumnAlias != null) {
					sb.Append(' ');
					sb.Append("AS");
					sb.Append(' ');
					sb.Append(Quote(ColumnAlias));
				}
			}
		}
	}
}