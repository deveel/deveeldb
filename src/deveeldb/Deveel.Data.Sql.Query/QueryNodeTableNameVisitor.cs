using System;
using System.Collections.Generic;
namespace Deveel.Data.Sql.Query {
	internal class QueryNodeTableNameVisitor : QueryPlanNodeVisitor {
		private IList<ObjectName> tableNames;

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
			if (node.RightExpression != null)
				node.RightExpression.DiscoverTableNames(tableNames);

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
	}
}
