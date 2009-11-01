using System;
using System.Text;

namespace Deveel.Data.Entity {
	internal class ColumnSortExpression : Expression {
		public ColumnSortExpression(VariableExpression columnName, bool ascending) {
			ColumnName = columnName;
			Ascending = ascending;
		}

		public ColumnSortExpression() {	
		}

		public VariableExpression ColumnName { get; set; }
		public bool Ascending { get; set; }

		internal override void WriteTo(StringBuilder sb) {
			sb.Append(Quote(ColumnName.ColumnName));
			sb.Append(' ');
			sb.Append(Ascending ? "ASC" : "DESC");
		}
	}
}