using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Statements.Build {
	class CreateTableStatementBuilder : ICreateTableStatementBuilder, IStatementBuilder {
		private ObjectName name;
		private bool temporary;
		private bool ifNotExists;
		private Dictionary<string, SqlTableColumn> columns;
		private Dictionary<string, SqlTableConstraint> constraints;

		public CreateTableStatementBuilder() {
			columns = new Dictionary<string, SqlTableColumn>();
			constraints = new Dictionary<string, SqlTableConstraint>();
		}


		public ICreateTableStatementBuilder Named(ObjectName tableName) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");

			name = tableName;
			return this;
		}

		public ICreateTableStatementBuilder WithColumn(SqlTableColumn column) {
			if (column == null)
				throw new ArgumentNullException("column");

			if (columns.ContainsKey(column.ColumnName))
				throw new ArgumentException(String.Format("A column named '{0}' was already specified", column.ColumnName));

			columns[column.ColumnName] = column;
			return this;
		}

		public ICreateTableStatementBuilder WithConstraint(SqlTableConstraint constraint) {
			if (constraint != null) {
				var constraintName = String.IsNullOrEmpty(constraint.ConstraintName) ? "#ANON#" : constraint.ConstraintName;
				SqlTableConstraint existing;
				if (constraints.TryGetValue(constraintName, out existing)) {
					var constraintColumns = new List<string>(existing.Columns);
					foreach (var column in constraint.Columns) {
						if (!constraintColumns.Contains(column))
							constraintColumns.Add(column);
					}

					constraints[constraintName] = new SqlTableConstraint(constraintName, existing.ConstraintType, constraintColumns.ToArray()) {
						CheckExpression = constraint.CheckExpression,
						OnDelete = constraint.OnDelete,
						OnUpdate = constraint.OnUpdate,
						ReferenceTable = constraint.ReferenceTable
					};
				} else {
					constraints[constraintName] = constraint;
				}
			}

			return this;
		}

		public ICreateTableStatementBuilder IfNotExists(bool value = true) {
			ifNotExists = value;
			return this;
		}

		public ICreateTableStatementBuilder Temporary(bool value = true) {
			temporary = value;
			return this;
		}

		public IEnumerable<SqlStatement> Build() {
			if (name == null)
				throw new InvalidOperationException("The name of the table is required");

			var list = new List<SqlStatement> {
				new CreateTableStatement(name, columns.Select(x => x.Value)) {
					Temporary = temporary,
					IfNotExists = ifNotExists
				}
			};

			if (constraints.Count > 0) {
				foreach (var constraint in constraints.Values) {
					list.Add(new AlterTableStatement(name, new AddConstraintAction(constraint)));
				}
			}

			return list;
		}
	}
}
