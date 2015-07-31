using System;
using System.Collections.Generic;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Statements {
	public sealed class InsertSelectStatement : SqlStatement {
		public InsertSelectStatement(string tableName, IEnumerable<string> columnNames, SqlQueryExpression queryExpression) {
			TableName = tableName;
			ColumnNames = columnNames;
			QueryExpression = queryExpression;
		}

		public string TableName { get; private set; }

		public IEnumerable<string> ColumnNames { get; private set; }

		public SqlQueryExpression QueryExpression { get; private set; }

		protected override IPreparedStatement PrepareStatement(IExpressionPreparer preparer, IQueryContext context) {
			
			throw new NotImplementedException();
		}

		#region PreparedInsertStatement

		class Prepared : SqlPreparedStatement {
			internal Prepared(InsertSelectStatement source, ObjectName tableName, IEnumerable<string> columnNames, IQueryPlanNode queryPlan)
				: base(source) {
				TableName = tableName;
				ColumnNames = columnNames;
				QueryPlan = queryPlan;
			}

			public ObjectName TableName { get; private set; }

			public IQueryPlanNode QueryPlan { get; private set; }

			public IEnumerable<string> ColumnNames { get; private set; }

			protected override ITable ExecuteStatement(IQueryContext context) {
				throw new NotImplementedException();
			}
		}

		#endregion
	}
}
