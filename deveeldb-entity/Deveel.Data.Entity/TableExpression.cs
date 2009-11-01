using System;
using System.Collections.Generic;
using System.Data.Metadata.Edm;
using System.Text;

namespace Deveel.Data.Entity {
	internal class TableExpression : BranchExpression {
		public TableExpression() {
			Columns = new List<VariableExpression>();
		}

		public string Schema { get; set; }
		public string Table { get; set; }
		public Expression DefiningQuery { get; set; }
		public TypeUsage Type { get; set; }
		public List<VariableExpression> Columns { get; private set; }

		public override Expression GetProperty(string propertyName) {
			if (Columns.Count == 0)
				return null;
			for (int i = 0; i < Columns.Count; i++) {
				VariableExpression variable = Columns[i];
				if (variable.ColumnName == propertyName)
					return variable;
			}
			return null;
		}

		internal override void WriteTo(StringBuilder sb) {
			if (DefiningQuery != null) {
				sb.Append('(');
				DefiningQuery.WriteTo(sb);
				sb.Append(')');
			} else {
				sb.Append(Quote(Table));
			}
			base.WriteTo(sb);
		}
	}
}