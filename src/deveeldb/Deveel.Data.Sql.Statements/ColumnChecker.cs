using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Statements {
	abstract class ColumnChecker {
		public string StripTableName(string tableDomain, string column) {
			var index = column.IndexOf('.');

			if (index != -1) {
				var columnPrefix = column.Substring(0, index);
				if (!columnPrefix.Equals(tableDomain))
					throw new InvalidOperationException(String.Format("Column '{0}' is not within the expected table'{1}'", 
						column, tableDomain));

				column = column.Substring(index + 1);
			}

			return column;
		}

		public IEnumerable<string> StripColumnList(string tableDomain, IEnumerable<string> columnList) {
			return columnList.Select(x => StripTableName(tableDomain, x));
		}

		public abstract string ResolveColumnName(string columnName);

		public SqlExpression CheckExpression(SqlExpression expression) {
			var expChecker = new ExpressionChecker(this);
			return expChecker.Visit(expression);
		}

		public IEnumerable<string> CheckColumns(IEnumerable<string> columnNames) {
			var result = new List<string>();

			foreach (var columnName in columnNames) {
				var resolved = ResolveColumnName(columnName);
				if (resolved == null)
					throw new InvalidOperationException(String.Format("Column '{0}' not found in table.", columnName));

				result.Add(resolved);
			}

			return result.AsReadOnly();
		}

		public static ColumnChecker Default(IQueryContext context, ObjectName tableName) {
			var table = context.GetTable(tableName);
			if (table == null)
				throw new InvalidOperationException(String.Format("Table '{0}' not found in the context.", tableName));

			var tableInfo = table.TableInfo;
			var ignoreCase = context.IgnoreIdentifiersCase();

			return new DefaultChecker(tableInfo, ignoreCase);
		}

		#region DefaultChecker

		class DefaultChecker : ColumnChecker {
			private readonly TableInfo tableInfo;
			private readonly bool ignoreCase;

			public DefaultChecker(TableInfo tableInfo, bool ignoreCase) {
				this.tableInfo = tableInfo;
				this.ignoreCase = ignoreCase;
			}

			public override string ResolveColumnName(string columnName) {
				var comparison = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
				string foundColumn = null;

				foreach (var columnInfo in tableInfo) {
					if (foundColumn != null)
						throw new InvalidOperationException(String.Format("Column name '{0}' caused an ambiguous match in table.", columnName));

					if (String.Equals(columnInfo.ColumnName, columnName, comparison))
						foundColumn = columnInfo.ColumnName;
				}

				return foundColumn;
			}
		}

		#endregion

		#region ExpressionChecker

		class ExpressionChecker : SqlExpressionVisitor {
			private readonly ColumnChecker checker;

			public ExpressionChecker(ColumnChecker checker) {
				this.checker = checker;
			}

			public override SqlExpression VisitReference(SqlReferenceExpression reference) {
				var refName = reference.ReferenceName;
				var origColumn = refName.Name;
				var resolvedColumn = checker.ResolveColumnName(origColumn);
				if (resolvedColumn == null)
					throw new InvalidOperationException(String.Format("Column '{0} not found in table.", origColumn));

				if (!origColumn.Equals(resolvedColumn))
					refName = new ObjectName(refName.Parent, resolvedColumn);

				return SqlExpression.Reference(refName);
			}

			public override SqlExpression VisitQuery(SqlQueryExpression query) {
				throw new InvalidOperationException("Sub-queries are not permitted in a CHECK expression.");
			}
		}

		#endregion
	}
}
