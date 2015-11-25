// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using System.Collections.Generic;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Tables;

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

		protected override SqlStatement PrepareStatement(IRequest context) {
			var tableName = context.Query.ResolveTableName(TableName);
			if (!context.Query.TableExists(tableName))
				throw new ObjectNotFoundException(tableName);

			var queryExpression = new SqlQueryExpression(new[]{SelectColumn.Glob("*") });
			queryExpression.FromClause.AddTable(tableName.FullName);
			queryExpression.WhereExpression = WherExpression;

			var queryFrom = QueryExpressionFrom.Create(context, queryExpression);
			var queryPlan = context.Query.QueryContext.QueryPlanner().PlanQuery(context, queryExpression, null, null);

			var columns = new List<SqlAssignExpression>();
			foreach (var assignment in Assignments) {
				var columnName = ObjectName.Parse(assignment.ColumnName);

				var refName = queryFrom.ResolveReference(columnName);
				var expression = assignment.Expression.Prepare(queryFrom.ExpressionPreparer);

				var assign = SqlExpression.Assign(SqlExpression.Reference(refName), expression);
				columns.Add(assign);
			}

			return new Prepared(tableName, queryPlan, columns.ToArray(), Limit);
		}

		#region Prepared

		sealed class Prepared : SqlStatement {
			internal Prepared(ObjectName tableName, IQueryPlanNode queryPlan, SqlAssignExpression[] columns, int limit) {
				TableName = tableName;
				QueryPlan = queryPlan;
				Columns = columns;
				Limit = limit;
			}

			protected override bool IsPreparable {
				get { return false; }
			}

			public ObjectName TableName { get; private set; }

			public IQueryPlanNode QueryPlan { get; private set; }

			public SqlAssignExpression[] Columns { get; private set; }

			public int Limit { get; private set; }

			protected override ITable ExecuteStatement(IRequest context) {
				var updateCount = context.Query.UpdateTable(TableName, QueryPlan, Columns, Limit);
				return FunctionTable.ResultTable(context, updateCount);
			}
		}

		#endregion
	}
}
