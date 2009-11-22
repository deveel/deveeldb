using System;
using System.Collections;
using System.Text;

namespace Deveel.Data.Select {
	public sealed class FromClause {
		internal FromClause() {
		}

		private readonly JoiningSet joinSet = new JoiningSet();
		private readonly ArrayList fromTables = new ArrayList();
		private readonly ArrayList allTableNames = new ArrayList();

		private void AddTableDef(string tableName, FromTable table) {
			if (tableName != null) {
				if (allTableNames.Contains(tableName))
					throw new ApplicationException("Duplicate table name in FROM clause: " + tableName);
				allTableNames.Add(tableName);
			}

			joinSet.AddTable(table.Name);
			fromTables.Add(table);
		}

		public JoiningSet JoinSet {
			get { return joinSet; }
		}

		public ICollection Tables {
			get { return fromTables; }
		}

		public void AddTable(String table_name) {
			AddTableDef(table_name, new FromTable(table_name));
		}

		public void AddTable(string tableName, string alias) {
			AddTableDef(alias, new FromTable(tableName, alias));
		}

		public void AddTable(string tableName, SelectExpression select, string alias) {
			// This is an inner select in the FROM clause
			if (tableName == null && select != null) {
				if (alias == null) {
					AddTableDef(null, new FromTable(select));
				} else {
					AddTableDef(alias, new FromTable(select, alias));
				}
			}
				// This is a standard table reference in the FROM clause
			else if (tableName != null && select == null) {
				if (alias == null) {
					AddTable(tableName);
				} else {
					AddTable(tableName, alias);
				}
			}
				// Error
			else {
				throw new ApplicationException("Unvalid declaration parameters.");
			}
		}

		public void AddJoin(JoinType type) {
			joinSet.AddJoin(type);
		}

		public void AddPreviousJoin(JoinType type, string onExpression) {
			joinSet.AddPreviousJoin(type, onExpression);
		}

		public void AddJoin(JoinType type, string onExpression) {
			joinSet.AddJoin(type, onExpression);
		}

		public override string ToString() {
			StringBuilder sb = new StringBuilder();
			DumpTo(sb);
			return sb.ToString();
		}

		internal void DumpTo(StringBuilder sb) {
			sb.Append("FROM ");

			FromTable fromTable = (FromTable)fromTables[0];
			fromTable.DumpTo(sb);

			for (int i = 1; i < fromTables.Count; i++) {
				fromTable = (FromTable) fromTables[i];
				JoinType joinType = JoinSet.GetJoinType(i);
				string onExpression = JoinSet.GetOnExpression(i);

				if (onExpression == null || onExpression.Length == 0) {
					sb.Append(", ");
				} else if (joinType == JoinType.Inner) {
					sb.Append("INNER JOIN ");
				} else if (joinType == JoinType.LeftOuter) {
					sb.Append("LEFT OUTER ");
				} else if (joinType == JoinType.RightOuter) {
					sb.Append("RIGHT OUTER ");
				} else if (joinType == JoinType.FullOuter) {
					sb.Append("FULL OUTER ");
				}

				fromTable.DumpTo(sb);

				if (onExpression != null && onExpression.Length > 0) {
					sb.Append("ON ");
					sb.Append(onExpression);
				}
			}
		}
	}
}