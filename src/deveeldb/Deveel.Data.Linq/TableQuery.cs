using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Linq {
	class TableQuery {
		private readonly List<ColumnQuery> columnQueries;

		public TableQuery() {
			columnQueries = new List<ColumnQuery>();
		}

		public void Column(string columnName, SqlExpression expression) {
			columnQueries.Add(new ColumnQuery(columnName, expression));
		}

		public void Column(string columnName, Expression expression) {
			// TODO: convert the source Linq Expression to a SQL Expression
			throw new NotImplementedException();
		}

		public IEnumerable Execute(Type elementType, ITable table) {
			var mapping = new TableTypeMapper(elementType);

			var finalTable = table;
			foreach (var columnQuery in columnQueries) {
				finalTable = ExecuteColumnQuery(finalTable, columnQuery);
			}

			mapping.BuildMap(finalTable);

			var listType = typeof (List<>).MakeGenericType(elementType);
			var result = (IList) Activator.CreateInstance(listType);

			foreach (var row in finalTable) {
				var rowNumber = row.RowId.RowNumber;
				var mapped = mapping.Construct(finalTable, rowNumber);
				result.Add(mapped);
			}

			return result;
		}

		private ITable ExecuteColumnQuery(ITable table, ColumnQuery columnQuery) {
			var expression = columnQuery.Expression;
			return table.ExhaustiveSelect(null, expression);
		}
	}
}
