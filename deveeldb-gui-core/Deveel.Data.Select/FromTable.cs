using System;
using System.Text;

namespace Deveel.Data.Select {
	public sealed class FromTable {
		public FromTable(string tableName, string tableAlias) {
			this.tableName = tableName;
			this.tableAlias = tableAlias;
			subselectTable = null;
			subqueryTable = false;
		}

		public FromTable(string tableName)
			: this(tableName, null) {
		}

		public FromTable(SelectExpression select, string tableAlias) {
			subselectTable = select;
			tableName = tableAlias;
			this.tableAlias = tableAlias;
			subqueryTable = true;
		}

		public FromTable(SelectExpression select) {
			subselectTable = select;
			tableName = null;
			tableAlias = null;
			subqueryTable = true;
		}

		private readonly bool subqueryTable;
		private readonly string tableName;
		private readonly string tableAlias;
		private readonly SelectExpression subselectTable;

		public SelectExpression SelectExpression {
			get { return subselectTable; }
		}

		public bool SubqueryTable {
			get { return subqueryTable; }
		}

		public string Alias {
			get { return tableAlias; }
		}

		public string Name {
			get { return tableName; }
		}

		internal void DumpTo(StringBuilder sb) {
			sb.Append(tableName);

			if (subqueryTable) {
				sb.Append(" ");
				sb.Append("(");
				subselectTable.DumpTo(sb, true);
				sb.Append(")");
			}

			if (tableAlias != null && tableAlias.Length > 0) {
				sb.Append(" AS ");
				sb.Append(tableAlias);
			}
		}
	}
}