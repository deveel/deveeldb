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

		protected override SqlStatement PrepareStatement(IExpressionPreparer preparer, IQueryContext context) {
			
			throw new NotImplementedException();
		}

		#region PreparedInsertStatement

		class Prepared : SqlStatement {
			internal Prepared(ObjectName tableName, IEnumerable<string> columnNames, IQueryPlanNode queryPlan) {
				TableName = tableName;
				ColumnNames = columnNames;
				QueryPlan = queryPlan;
			}

			public ObjectName TableName { get; private set; }

			public IQueryPlanNode QueryPlan { get; private set; }

			public IEnumerable<string> ColumnNames { get; private set; }

			protected override bool IsPreparable {
				get { return false; }
			}

			protected override ITable ExecuteStatement(IQueryContext context) {
				throw new NotImplementedException();
			}
		}

		#endregion
	}
}
