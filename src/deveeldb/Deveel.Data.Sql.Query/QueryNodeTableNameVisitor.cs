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
namespace Deveel.Data.Sql.Query {
	internal class QueryNodeTableNameVisitor : QueryPlanNodeVisitor {
		private readonly IList<ObjectName> tableNames;

		public QueryNodeTableNameVisitor() {
			tableNames = new List<ObjectName>();
		}

		public IList<ObjectName> Discover(IQueryPlanNode queryPlan) {
			VisitNode(queryPlan);
			return tableNames;
		}

		protected override IQueryPlanNode VisitFetchTable(FetchTableNode node) {
			if (!tableNames.Contains(node.TableName))
				tableNames.Add(node.TableName);

			return base.VisitFetchTable(node);
		}

		protected override IQueryPlanNode VisitFetchView(FetchViewNode node) {
			if (!tableNames.Contains(node.ViewName))
				tableNames.Add(node.ViewName);

			return base.VisitFetchView(node);
		}

		protected override IQueryPlanNode VisitJoin(JoinNode node) {
			if (node.RightExpression != null)
				node.RightExpression.DiscoverTableNames(tableNames);

			return base.VisitJoin(node);
		}

		protected override IQueryPlanNode VisitConstantSelect(ConstantSelectNode node) {
			if (node.Expression != null)
				node.Expression.DiscoverTableNames(tableNames);

			return base.VisitConstantSelect(node);
		}

		protected override IQueryPlanNode VisitRangeSelect(RangeSelectNode node) {
			if (node.Expression != null)
				node.Expression.DiscoverTableNames(tableNames);

			return base.VisitRangeSelect(node);
		}

		protected override IQueryPlanNode VisitSimpleSelect(SimpleSelectNode node) {
			if (node.Expression != null)
				node.Expression.DiscoverTableNames(tableNames);

			return base.VisitSimpleSelect(node);
		}

		protected override IQueryPlanNode VisitGroup(GroupNode node) {
			if (node.Functions != null) {
				foreach (var function in node.Functions) {
					function.DiscoverTableNames(tableNames);
				}
			}

			return base.VisitGroup(node);
		}

		protected override IQueryPlanNode VisitExhaustiveSelect(ExhaustiveSelectNode node) {
			if (node.Expression != null)
				node.Expression.DiscoverTableNames(tableNames);

			return base.VisitExhaustiveSelect(node);
		}

		protected override IQueryPlanNode VisitSimplePatternSelect(SimplePatternSelectNode node) {
			if (node.Expression != null)
				node.Expression.DiscoverTableNames(tableNames);

			return base.VisitSimplePatternSelect(node);
		}
	}
}
