using System;
using System.Collections.Generic;
using System.Data.Common.CommandTrees;
using System.Data.Metadata.Edm;

namespace Deveel.Data.Entity {
	internal class SelectExpression : BranchExpression {
		        private Dictionary<string, VariableExpression> columnHash;

        public SelectExpression() {
            Columns = new List<VariableExpression>();
        }

        public BranchExpression From;
        public List<VariableExpression> Columns { get; private set;  }
		public Expression Where { get; set; }
        public List<Expression> GroupBy { get; private set; }
        public List<Expression> OrderBy { get; private set; }
		public bool IsDistinct { get; set; }

		public void AddGroupBy(Expression e) {
			if (GroupBy == null)
				GroupBy = new List<Expression>();
			GroupBy.Add(e);
		}

		public void AddOrderBy(Expression e) {
			if (OrderBy == null)
				OrderBy = new List<Expression>();
			OrderBy.Add(e);
		}

		public override Expression GetProperty(string propertyName) {
			if (From == null || From.Name != propertyName) 
				return null;
			return From;
		}

		internal override void WriteTo(System.Text.StringBuilder sb) {
			if (Scoped)
				sb.Append("(");
			sb.Append("SELECT");
			if (IsDistinct)
				sb.Append(" DISTINCT ");
			WriteList(Columns, sb);

			if (From != null) {
				sb.AppendLine();
				sb.Append("FROM ");
				From.WriteTo(sb);
			}
			if (Where != null) {
				sb.AppendLine();
				sb.Append(" WHERE ");
				Where.WriteTo(sb);
			}
			if (GroupBy != null) {
				sb.AppendLine();
				sb.Append(" GROUP BY ");
				WriteList(GroupBy, sb);
			}
			if (OrderBy != null) {
				sb.AppendLine();
				sb.Append(" ORDER BY ");
				WriteList(OrderBy, sb);
			}
			if (Scoped) {
				sb.Append(")");
				if (Name != null) {
					sb.Append(' ');
					sb.Append("AS");
					sb.Append(' ');
					sb.Append(Quote(Name));
				}
			}
		}

		internal override void InsertInScope(Deveel.Data.Entity.ExpressionScope scope) {
			base.InsertInScope(scope);

			// next we need to remove child extents of the select from scope
			if (Name != null) {
				scope.Remove(this);
				scope.Add(Name, this);
			}

			// now we need to add default columns if necessary
			if (Columns.Count == 0)
				AddDefaultColumns();

		}

		void AddDefaultColumns() {
			AddDefaultColumnsForFragment(From);
		}

		void AddDefaultColumnsForFragment(BranchExpression input) {
			if (input is TableExpression) {
				AddDefaultColumnsForTable(input as TableExpression);
			} else if (input is JoinExpression) {
				JoinExpression j = input as JoinExpression;
				AddDefaultColumnsForFragment(j.Left);
				AddDefaultColumnsForFragment(j.Right);
			} else
				throw new NotImplementedException();
		}

		void AddDefaultColumnsForTable(TableExpression table) {
			if (columnHash == null)
				columnHash = new Dictionary<string, VariableExpression>();

			foreach (EdmProperty property in Metadata.GetProperties(table.Type.EdmType)) {
				VariableExpression col = new VariableExpression {TableName = table.Name, ColumnName = property.Name};
				table.Columns.Add(col);
				if (columnHash.ContainsKey(col.ColumnName)) {
					col.ColumnAlias = MakeColumnNameUnique(col.ColumnName);
					columnHash.Add(col.ColumnAlias, col);
				} else
					columnHash.Add(col.ColumnName, col);
				Columns.Add(col);
			}
		}

		private string MakeColumnNameUnique(string baseName) {
			int i = 1;
			while (true) {
				string name = baseName + i;
				if (!columnHash.ContainsKey(name))
					return name;
				i++;
			}
		}

		public bool IsCompatible(DbExpressionKind expressionKind) {
			switch (expressionKind) {
				case DbExpressionKind.Filter:
					return Where == null && Columns.Count == 0;
				case DbExpressionKind.Project:
					return Columns.Count == 0;
				case DbExpressionKind.Sort:
					return Columns.Count == 0 &&
					       GroupBy == null &&
					       OrderBy == null;
				case DbExpressionKind.GroupBy:
					return Columns.Count == 0 &&
					       GroupBy == null &&
					       OrderBy == null;
			}
			throw new InvalidOperationException();
		}
	}
}