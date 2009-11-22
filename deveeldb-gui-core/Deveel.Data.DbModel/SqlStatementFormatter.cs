using System;
using System.Collections;
using System.Text;

using Deveel.Data.Select;

namespace Deveel.Data.DbModel {
	public sealed class SqlStatementFormatter : ISqlStatementFormatter {
		private bool useParameterNames;

		public bool UseParameterNames {
			get { return useParameterNames; }
			set { useParameterNames = value; }
		}

		#region Implementation of ISqlStatementFormatter

		public string FormatColumnDeclaration(DbColumn column) {
			StringBuilder sb = new StringBuilder();
			sb.Append(column.Name);
			sb.Append(" ");
			sb.Append(column.DataType.Name);
			
			if (column.Size != -1) {
				sb.Append("(");
				sb.Append(column.Size);
				if (column.Scale != -1) {
					sb.Append(", ");
					sb.Append(column.Scale);
				}

				sb.Append(")");
			}

			if (!column.Nullable)
				sb.Append(" NOT NULL");

			if (column.Default != null && column.Default.Length > 0) {
				sb.Append(" DEFAULT ");
				sb.Append(column.Default);
			}

			return sb.ToString();
		}

		public string FormatConstraintDeclaration(DbConstraint constraint) {
			StringBuilder sb = new StringBuilder();

			if (constraint.Name != null && constraint.Name.Length > 0) {
				sb.Append(constraint.Name);
				sb.Append(" ");
			}

			if (constraint is DbPrimaryKey) {
				DbPrimaryKey primaryKey = (DbPrimaryKey) constraint;
				sb.Append("PRIMARY KEY ");
				sb.Append("(");
				for (int i = 0; i < primaryKey.Columns.Count; i++) {
					DbColumn column = (DbColumn) primaryKey.Columns[i];
					sb.Append(column.Name);
					if (i < primaryKey.Columns.Count - 1)
						sb.Append(", ");
				}
				sb.Append(")");
			} else if (constraint is DbUniqueConstraint) {
				DbUniqueConstraint unique = (DbUniqueConstraint) constraint;
				sb.Append("UNIQUE ");
				sb.Append("(");
				for (int i = 0; i < unique.Columns.Count; i++) {
					DbColumn column = (DbColumn)unique.Columns[i];
					sb.Append(column.Name);
					if (i < unique.Columns.Count - 1)
						sb.Append(", ");
				}

				sb.Append(")");
			} else if (constraint is DbCheckConstraint) {
				sb.Append("CHECK ");
				sb.Append("(");
				sb.Append(((DbCheckConstraint) constraint).Expression);
				sb.Append(")");
			} else if (constraint is DbForeignKey) {
				DbForeignKey foreignKey = (DbForeignKey) constraint;

				sb.Append("FOREIGN KEY ");
				sb.Append("(");
				for (int i = 0; i < foreignKey.Columns.Count; i++) {
					DbColumn column = (DbColumn)foreignKey.Columns[i];
					sb.Append(column.Name);
					if (i < foreignKey.Columns.Count - 1)
						sb.Append(", ");
				}
				sb.Append(")");

				sb.Append(" REFERENCES ");
				if (foreignKey.ReferenceSchema != null && foreignKey.ReferenceSchema.Length > 0) {
					sb.Append(foreignKey.ReferenceSchema);
					sb.Append(".");
				}
				sb.Append(foreignKey.ReferenceTable);
				sb.Append("(");
				for (int i = 0; i < foreignKey.ReferenceColumns.Count; i++) {
					DbColumn column = (DbColumn)foreignKey.ReferenceColumns[i];
					sb.Append(column.Name);
					if (i < foreignKey.ReferenceColumns.Count - 1)
						sb.Append(", ");
				}

				sb.Append(")");

				if (foreignKey.OnUpdate != null && foreignKey.OnUpdate.Length > 0) {
					sb.Append(" ON UPDATE ");
					sb.Append(foreignKey.OnUpdate);
				}

				if (foreignKey.OnDelete != null && foreignKey.OnDelete.Length > 0) {
					sb.Append(" ON DELETE ");
					sb.Append(foreignKey.OnDelete);
				}
			}

			return sb.ToString();
		}

		public string FormatCreateTable(DbTable table, bool ifNotExists) {
			StringBuilder sb = new StringBuilder();
			sb.Append("CREATE TABLE ");
			sb.Append(table.FullName);
			sb.Append(" ");

			if (ifNotExists)
				sb.Append("IF NOT EXISTS ");

			sb.AppendLine("(");

			IList columns = table.Columns;
			for (int i = 0; i < columns.Count; i++) {
				DbColumn column = (DbColumn) columns[i];
				sb.Append(FormatColumnDeclaration(column));
				if (i < columns.Count - 1)
					sb.AppendLine(", ");
			}

			IList constraints = table.Constraints;
			if (constraints.Count > 0) {
				sb.AppendLine(", ");

				for (int i = 0; i < constraints.Count; i++) {
					DbConstraint constraint = (DbConstraint) constraints[i];
					sb.Append(FormatConstraintDeclaration(constraint));
					if (i < constraints.Count - 1)
						sb.AppendLine(", ");
				}
			}

			sb.AppendLine();
			sb.Append(")");
			return sb.ToString();
		}

		public string FormatCreateView(string name, SelectExpression expression) {
			StringBuilder sb = new StringBuilder();
			sb.Append("CREATE VIEW ");
			sb.Append(name);
			sb.Append(" AS ");
			sb.Append(expression.ToString());
			return sb.ToString();
		}

		private static IList GetInsertColumns(DbTable table, DbTableValues values) {
			ArrayList list = new ArrayList();

			IList columns = table.Columns;
			for (int i = 0; i < columns.Count; i++) {
				DbColumn column = (DbColumn) columns[i];
				if (column.Identity)
					continue;

				if (!values.IsSet(column.Name))
					continue;

				list.Add(column);
			}
			return list;
		}

		public string FormatInsert(DbTable table, DbTableValues values) {
			StringBuilder sb = new StringBuilder();

			IList columns = GetInsertColumns(table, values);

			sb.Append("INSERT INTO ");
			sb.Append(table.FullName);
			sb.AppendLine("(");

			for (int i = 0; i < columns.Count; i++) {
				DbColumn column = (DbColumn) columns[i];
				sb.Append(column.Name);

				if (i < columns.Count - 1)
					sb.AppendLine(", ");
			}

			sb.AppendLine(") VALUES (");

			for (int i = 0; i < columns.Count; i++) {
				DbColumn column = (DbColumn) columns[i];

				if (values.IsDefault(column.Name)) {
					sb.Append("DEFAULT");
				} else if (useParameterNames) {
					sb.Append("@");
					sb.Append(column.Name);
				} else {
					sb.Append(column.DataType.LiteralPrefix);
					object value = values.GetValue(column.Name);
					//TODO: format according to the column data type ...
					sb.Append(Convert.ToString(value));
					sb.Append(column.DataType.LiteralSuffix);
				}

				if (i < columns.Count - 1)
					sb.AppendLine(", ");
			}

			sb.AppendLine();
			sb.Append(")");

			return sb.ToString();
		}

		private static IList GetUpdateColumns(DbTable table, DbTableValues values) {
			ArrayList list = new ArrayList();

			IList columns = table.Columns;
			for (int i = 0; i < columns.Count; i++) {
				DbColumn column = (DbColumn) columns[i];

				if (values.IsSet(column.Name))
					columns.Add(column);
			}

			return list;
		}

		private static IList GetSearchColumns(DbTable table, DbTableValues values) {
			ArrayList list = new ArrayList();

			DbConstraint[] constraints = table.GetConstraints(DbConstraintType.PrimaryKey);
			if (constraints.Length > 1)
				throw new NotSupportedException();

			if (constraints.Length == 0) {
				DbColumn column = table.IdentityColumn;
				if (column != null && values.IsSet(column.Name))
					list.Add(column);
			} else {
				DbPrimaryKey primaryKey = constraints[0] as DbPrimaryKey;
				if (primaryKey != null) {
					for (int i = 0; i < primaryKey.Columns.Count; i++) {
						DbColumn column = (DbColumn) primaryKey.Columns[i];
						if (values.IsSet(column.Name))
							list.Add(column);
					}
				}
			}

			return list;
		}

		public string FormatDelete(DbTable table, DbTableValues values) {
			if (values == null || values.Count == 0)
				throw new ArgumentException();

			StringBuilder sb = new StringBuilder();

			sb.Append("DELETE FROM ");
			sb.Append(table.FullName);

			sb.Append(" WHERE ");

			IList columns = GetSearchColumns(table, values);
			for (int i = 0; i < columns.Count; i++) {
				DbColumn column = (DbColumn) columns[i];

				if (useParameterNames) {
					sb.Append("@");
					sb.Append(column.Name);
				} else {
					sb.Append(column.Name);
					sb.Append("=");
					sb.Append(column.DataType.LiteralPrefix);
					//TODO: convert better...
					object value = values.GetValue(column.Name);
					sb.Append(Convert.ToString(value));
					sb.Append(column.DataType.LiteralSuffix);
				}

				if (i < columns.Count - 1)
					sb.Append(" AND ");
			}

			return sb.ToString();
		}

		public string FormatUpdate(DbTable table, DbTableValues values) {
			if (values == null || values.Count == 0)
				throw new ArgumentException();

			StringBuilder sb = new StringBuilder();

			sb.Append("UPDATE ");
			sb.Append(table.FullName);
			sb.Append(" SET ");

			IList columns = GetUpdateColumns(table, values);

			for (int i = 0; i < columns.Count; i++) {
				DbColumn column = (DbColumn) columns[i];

				if (useParameterNames) {
					sb.Append("@");
					sb.Append(column.Name);
				} else {
					sb.Append(column.Name);
					sb.Append("=");
					sb.Append(column.DataType.LiteralPrefix);
					//TODO: convert better...
					object value = values.GetValue(column.Name);
					sb.Append(Convert.ToString(value));
					sb.Append(column.DataType.LiteralSuffix);
				}

				if (i < columns.Count - 1)
					sb.Append(", ");
			}

			columns = GetSearchColumns(table, values);
			if (columns.Count > 0) {
				for (int i = 0; i < columns.Count; i++) {
					DbColumn column = (DbColumn)columns[i];

					if (useParameterNames) {
						sb.Append("@");
						sb.Append(column.Name);
					} else {
						sb.Append(column.Name);
						sb.Append("=");
						sb.Append(column.DataType.LiteralPrefix);
						//TODO: convert better...
						object value = values.GetValue(column.Name);
						sb.Append(Convert.ToString(value));
						sb.Append(column.DataType.LiteralSuffix);
					}

					if (i < columns.Count - 1)
						sb.Append(" AND ");
				}
			}

			return sb.ToString();
		}

		#endregion
	}
}