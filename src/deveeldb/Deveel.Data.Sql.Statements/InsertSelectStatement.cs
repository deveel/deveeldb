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
using System.Linq;

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public sealed class InsertSelectStatement : SqlStatement, IPreparableStatement {
		public InsertSelectStatement(string tableName, IEnumerable<string> columnNames, SqlQueryExpression queryExpression) {
			TableName = tableName;
			ColumnNames = columnNames;
			QueryExpression = queryExpression;
		}

		public string TableName { get; private set; }

		public IEnumerable<string> ColumnNames { get; private set; }

		public SqlQueryExpression QueryExpression { get; private set; }

		IStatement IPreparableStatement.Prepare(IRequest request) {
			var tableName = request.Query.ResolveTableName(TableName);
			if (tableName == null)
				throw new ObjectNotFoundException(ObjectName.Parse(TableName));

			var columns = new string[0];
			if (ColumnNames != null)
				columns = ColumnNames.ToArray();

			// TODO: Verify the columns!!!

			var queryPlan = request.Context.QueryPlanner().PlanQuery(new QueryInfo(request, QueryExpression));
			return new Prepared(tableName, columns, queryPlan);
		}

		#region PreparedInsertStatement

		[Serializable]
		class Prepared : SqlStatement {
			internal Prepared(ObjectName tableName, string[] columnNames, IQueryPlanNode queryPlan) {
				TableName = tableName;
				ColumnNames = columnNames;
				QueryPlan = queryPlan;
			}

			private Prepared(ObjectData data) {
				TableName = data.GetValue<ObjectName>("TableName");
				QueryPlan = data.GetValue<IQueryPlanNode>("QueryPlan");
				ColumnNames = data.GetValue<string[]>("ColumnNames");
			}

			public ObjectName TableName { get; private set; }

			public IQueryPlanNode QueryPlan { get; private set; }

			public string[] ColumnNames { get; private set; }

			protected override void GetData(SerializeData data) {
				data.SetValue("TableName", TableName);
				data.SetValue("QueryPlan", QueryPlan);
				data.SetValue("ColumnNames", ColumnNames);
			}

			protected override void ExecuteStatement(ExecutionContext context) {
				throw new NotImplementedException();
			}
		}

		#endregion
	}
}
