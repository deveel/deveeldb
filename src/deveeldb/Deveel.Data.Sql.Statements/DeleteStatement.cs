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
using System.Runtime.Serialization;

using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DeleteStatement : SqlStatement, IPlSqlStatement {
		public DeleteStatement(ObjectName tableName, SqlExpression whereExpression) 
			: this(tableName, whereExpression, -1) {
		}

		public DeleteStatement(ObjectName tableName, SqlExpression whereExpression, long limit) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");
			if (whereExpression == null)
				throw new ArgumentNullException("whereExpression");

			if (limit <= 0)
				limit = -1;

			TableName = tableName;
			WhereExpression = whereExpression;
			Limit = limit;
		}

		private DeleteStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			TableName = (ObjectName) info.GetValue("TableName", typeof (ObjectName));
			WhereExpression = (SqlExpression) info.GetValue("Where", typeof (SqlExpression));
			Limit = info.GetInt64("Limit");
		}

		public ObjectName TableName { get; private set; }

		public SqlExpression WhereExpression { get; private set; }

		public long Limit { get; set; }

		protected override void GetData(SerializationInfo info) {
			info.AddValue("TableName", TableName);
			info.AddValue("Where", WhereExpression);
			info.AddValue("Limit", Limit);
		}

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			var where = WhereExpression.Prepare(preparer);
			return new DeleteStatement(TableName, where, Limit);
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			var tableName = context.Access.ResolveTableName(TableName);

			if (!context.Access.TableExists(tableName))
				throw new ObjectNotFoundException(tableName);

			var queryExp = new SqlQueryExpression(new SelectColumn[] {SelectColumn.Glob("*") });
			queryExp.FromClause.AddTable(tableName.FullName);
			queryExp.WhereExpression = WhereExpression;

			var queryInfo = new QueryInfo(context, queryExp);
			if (Limit > 0)
				queryInfo.Limit = new QueryLimit(Limit);

			var queryPlan = context.Query.Context.QueryPlanner().PlanQuery(queryInfo);

			return new Prepared(tableName, queryPlan);
		}

		#region Prepared

		[Serializable]
		class Prepared : SqlStatement {
			public Prepared(ObjectName tableName, IQueryPlanNode queryPlan) {
				TableName = tableName;
				QueryPlan = queryPlan;
			}

			private Prepared(SerializationInfo info, StreamingContext context)
				: base(info, context) {
				TableName = (ObjectName) info.GetValue("TableName", typeof(ObjectName));
				QueryPlan = (IQueryPlanNode) info.GetValue("QueryPlan", typeof(IQueryPlanNode));
			}

			public ObjectName TableName { get; private set; }

			public IQueryPlanNode QueryPlan { get; private set; }

			protected override void GetData(SerializationInfo info) {
				info.AddValue("TableName", TableName);
				info.AddValue("QueryPlan", QueryPlan);
			}

			protected override void ExecuteStatement(ExecutionContext context) {
				var deleteTable = context.Request.Access.GetMutableTable(TableName);

				if (deleteTable == null)
					throw new ObjectNotFoundException(TableName);

				if (!context.User.CanSelectFrom(QueryPlan))
					throw new MissingPrivilegesException(context.User.Name, TableName, Privileges.Select);
				if (!context.User.CanDeleteFromTable(TableName))
					throw new MissingPrivilegesException(context.User.Name, TableName, Privileges.Delete);
				
				var result = QueryPlan.Evaluate(context.Request);
				var count = deleteTable.Delete(result);

				context.SetResult(count);
			}
		}

		#endregion
	}
}
