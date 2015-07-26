using System;
using System.Collections.Generic;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Statements {
	public sealed class UpdateStatement : SqlStatement {
		public UpdateStatement(string tableName, SqlExpression wherExpression, IEnumerable<SqlColumnAssignment> assignments) {
			if (wherExpression == null)
				throw new ArgumentNullException("wherExpression");
			if (assignments == null)
				throw new ArgumentNullException("assignments");
			if (String.IsNullOrEmpty(tableName))
				throw new ArgumentNullException("tableName");

			TableName = tableName;
			WherExpression = wherExpression;
			Assignments = assignments;
		}

		public string TableName { get; private set; }

		public SqlExpression WherExpression { get; private set; }

		public int Limit { get; set; }

		public IEnumerable<SqlColumnAssignment> Assignments { get; private set; }

		protected override SqlPreparedStatement PrepareStatement(IExpressionPreparer preparer, IQueryContext context) {
			var tableName = context.ResolveTableName(TableName);
			if (!context.TableExists(tableName))
				throw new ObjectNotFoundException(tableName);

			var queryExpression = new SqlQueryExpression(new[]{SelectColumn.Glob("*") });
			queryExpression.FromClause.AddTable(tableName.FullName);
			queryExpression.WhereExpression = WherExpression;

			var queryFrom = QueryExpressionFrom.Create(context, queryExpression);
			var queryPlan = context.DatabaseContext().QueryPlanner().PlanQuery(context, queryExpression, null);

			var columns = new List<SqlAssignExpression>();
			foreach (var assignment in Assignments) {
				var columnName = ObjectName.Parse(assignment.ColumnName);

				var refName = queryFrom.ResolveReference(columnName);
				var expression = assignment.Expression.Prepare(queryFrom.ExpressionPreparer);

				var assign = SqlExpression.Assign(SqlExpression.Reference(refName), expression);
				columns.Add(assign);
			}

			return new PreparedUpdateStatement(tableName, queryPlan, columns.ToArray(), Limit);
		}

		#region PreparedUpdateStatement

		class PreparedUpdateStatement : SqlPreparedStatement {
			public PreparedUpdateStatement(ObjectName tableName, IQueryPlanNode queryPlan, SqlAssignExpression[] columns, int limit) {
				TableName = tableName;
				QueryPlan = queryPlan;
				Columns = columns;
				Limit = limit;
			}

			public ObjectName TableName { get; private set; }

			public IQueryPlanNode QueryPlan { get; private set; }

			public SqlAssignExpression[] Columns { get; private set; }

			public int Limit { get; private set; }

			public override ITable Evaluate(IQueryContext context) {
				var updateCount = context.UpdateTable(TableName, QueryPlan, Columns, Limit);
				return FunctionTable.ResultTable(context, updateCount);
			}
		}

		#endregion
	}
}
