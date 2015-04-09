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

namespace Deveel.Data.Sql.Expressions {
	public sealed class SqlQueryExpression : SqlExpression {
		public SqlQueryExpression(IEnumerable<SelectColumn> selectColumns) {
			SelectColumns = selectColumns;
			FromClause = new FromClause();
		}

		public IEnumerable<SelectColumn> SelectColumns { get; private set; }

		//TODO: Group By Columns 

		public FromClause FromClause { get; internal set; }

		public SqlExpression WhereExpression { get; set; }

		public SqlExpression HavingExpression { get; set; }

		public IEnumerable<SqlExpression> GroupBy { get; set; } 

		public SqlQueryExpression NextComposite { get; set; }

		public override SqlExpressionType ExpressionType {
			get { return SqlExpressionType.Query; }
		}

		public override bool CanEvaluate {
			get { return true; }
		}

		public override SqlExpression Evaluate(EvaluateContext context) {
			var queryContext = context.QueryContext;
			var planContext = queryContext.Session;
			var planner = context.DatabaseContext.QueryPlanner;
			var plan = planner.PlanQuery(planContext, this);

			return new SqlPreparedQueryExpression(plan);
		}
	}
}