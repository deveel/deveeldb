using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class InsertValuesStatement : SqlStatement {
		public InsertValuesStatement(string tableName, IEnumerable<string> columnNames, IEnumerable<SqlExpression[]> values) {
			if (columnNames == null)
				throw new ArgumentNullException("columnNames");
			if (values == null)
				throw new ArgumentNullException("values");
			if (String.IsNullOrEmpty(tableName))
				throw new ArgumentNullException("tableName");

			TableName = tableName;
			ColumnNames = columnNames;
			Values = values;
		}

		public string TableName { get; private set; }

		public IEnumerable<string> ColumnNames { get; private set; } 

		public IEnumerable<SqlExpression[]> Values { get; private set; } 

		public override StatementType StatementType {
			get { return StatementType.InsertValues; }
		}

		protected override SqlPreparedStatement PrepareStatement(IExpressionPreparer preparer, IQueryContext context) {
			var tableName = context.ResolveTableName(TableName);

			var table = context.GetTable(tableName);
			if (table == null)
				throw new InvalidOperationException();

			if (Values.Any(x => x.OfType<SqlQueryExpression>().Any()))
				throw new InvalidOperationException("Cannot set a value from a query.");

			var columnInfos = new List<ColumnInfo>();
			foreach (var name in ColumnNames) {
				var columnName = ObjectName.Parse(name);
				var colIndex = table.FindColumn(columnName);
				if (colIndex < 0)
					throw new InvalidOperationException(String.Format("Cannot find column '{0}' in table '{1}'", columnName, table.FullName));

				columnInfos.Add(table.TableInfo[colIndex]);
			}

			var assignments = new List<SqlAssignExpression[]>();

			foreach (var valueSet in Values) {
				var valueAssign = new SqlAssignExpression[valueSet.Length];

				for (int i = 0; i < valueSet.Length; i++) {
					var columnInfo = columnInfos[i];

					var value = valueSet[i];
					if (value != null) {
						// TODO: Deference columns with a preparer
					}

					if (value != null) {
						var expReturnType = value.ReturnType(context, null);
						if (!columnInfo.ColumnType.IsComparable(expReturnType))
							throw new InvalidOperationException();
					}

					valueAssign[i] = SqlExpression.Assign(SqlExpression.Reference(columnInfo.FullColumnName), value);
				}

				assignments.Add(valueAssign);
			}

			return new PreparedInsertStatement(tableName, assignments);
		}

		#region PreparedInsertStatement

		class PreparedInsertStatement : SqlPreparedStatement {
			public PreparedInsertStatement(ObjectName tableName, IEnumerable<SqlAssignExpression[]> assignments) {
				TableName = tableName;
				Assignments = assignments;
			}

			public ObjectName TableName { get; private set; }

			public IEnumerable<SqlAssignExpression[]> Assignments { get; private set; } 
 
			public override ITable Evaluate(IQueryContext context) {
				var insertCount = context.InsertIntoTable(TableName, Assignments);
				return FunctionTable.ResultTable(context, insertCount);
			}
		}

		#endregion
	}
}
