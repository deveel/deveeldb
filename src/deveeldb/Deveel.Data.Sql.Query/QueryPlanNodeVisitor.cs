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

namespace Deveel.Data.Sql.Query {
	class QueryPlanNodeVisitor : IQueryPlanNodeVisitor {
		IQueryPlanNode IQueryPlanNodeVisitor.Visit(IQueryPlanNode node) {
			return VisitNode(node);
		}

		protected virtual IQueryPlanNode VisitNode(IQueryPlanNode node) {
			if (node is SingleQueryPlanNode)
				return VisitSingle((SingleQueryPlanNode) node);
			if (node is BranchQueryPlanNode)
				return VisitBranch((BranchQueryPlanNode) node);
			if (node is FetchTableNode)
				return VisitFetchTable((FetchTableNode) node);
			if (node is FetchViewNode)
				return VisitFetchView((FetchViewNode) node);

			throw new NotSupportedException();
		}

		protected virtual IQueryPlanNode VisitFetchView(FetchViewNode node) {
			return node;
		}

		protected virtual IQueryPlanNode VisitFetchTable(FetchTableNode node) {
			return node;
		}

		protected virtual IQueryPlanNode VisitSingle(SingleQueryPlanNode node) {
			if (node is SimpleSelectNode) {
				return VisitSimpleSelect((SimpleSelectNode)node);
			} else if (node is ExhaustiveSelectNode) {
				return VisitExhaustiveSelect((ExhaustiveSelectNode)node);
			} else if (node is ConstantSelectNode) {
				return VisitConstantSelect((ConstantSelectNode)node);
			} else if (node is RangeSelectNode) {
				return VisitRangeSelect((RangeSelectNode)node);
			} else if (node is DistinctNode) {
				return VisitDistinct((DistinctNode)node);
			} else if (node is LeftOuterJoinNode) {
				return VisitLeftOuterJoin((LeftOuterJoinNode)node);
			} else if (node is CachePointNode) {
				return VisitCachePoint((CachePointNode)node);
			} else if (node is MarkerNode) {
				return VisitMarker((MarkerNode)node);
			}
			if (node is GroupNode)
				return VisitGroup((GroupNode) node);

			throw new NotSupportedException();
		}

		protected virtual IQueryPlanNode VisitBranch(BranchQueryPlanNode node) {
			if (node is CompositeNode)
				return VisitComposite((CompositeNode) node);
			if (node is JoinNode) {
				return VisitJoin((JoinNode) node);
			} else if (node is EquiJoinNode) {
				return VisitEquiJoin((EquiJoinNode) node);
			} else if (node is NaturalJoinNode) {
				return VisitNaturalJoin((NaturalJoinNode) node);
			}

			throw new NotSupportedException();
		}

		protected virtual IQueryPlanNode VisitJoin(JoinNode node) {
			var left = node.Left;
			var right = node.Right;
			if (left != null)
				left = VisitNode(left);
			if (right != null)
				right = VisitNode(right);

			return new JoinNode(left, right, node.LeftColumnName, node.Operator, node.RightExpression);
		}

		protected virtual IQueryPlanNode VisitNaturalJoin(NaturalJoinNode node) {
			var left = node.Left;
			var right = node.Right;
			if (left != null)
				left = VisitNode(left);
			if (right != null)
				right = VisitNode(right);

			return new NaturalJoinNode(left, right);
		}

		protected virtual IQueryPlanNode VisitEquiJoin(EquiJoinNode node) {
			var left = node.Left;
			var right = node.Right;
			if (left != null)
				left = VisitNode(left);
			if (right != null)
				right = VisitNode(right);

			return new EquiJoinNode(left, right, node.LeftColumns, node.RightColumns);
		}

		protected virtual IQueryPlanNode VisitComposite(CompositeNode node) {
			var left = node.Left;
			var right = node.Right;
			if (left != null)
				left = VisitNode(left);
			if (right != null)
				right = VisitNode(right);

			return new CompositeNode(left, right, node.CompositeFunction, node.All);
		}

		protected virtual IQueryPlanNode VisitMarker(MarkerNode node) {
			var child = node.Child;
			if (child != null)
				child = VisitNode(child);

			return new MarkerNode(child, node.MarkName);
		}

		protected virtual IQueryPlanNode VisitLeftOuterJoin(LeftOuterJoinNode node) {
			var child = node.Child;
			if (child != null)
				child = VisitNode(child);

			return new LeftOuterJoinNode(child, node.MarkerName);
		}

		protected virtual IQueryPlanNode VisitGroup(GroupNode node) {
			var child = node.Child;
			if (child != null)
				child = VisitNode(child);

			return new GroupNode(child, node.ColumnNames, node.GroupMaxColumn, node.Functions, node.Names);
		}

		protected virtual IQueryPlanNode VisitRangeSelect(RangeSelectNode node) {
			var child = node.Child;
			if (child != null)
				child = VisitNode(child);

			return new RangeSelectNode(child, node.Expression);
		}

		protected virtual IQueryPlanNode VisitCachePoint(CachePointNode node) {
			var child = node.Child;
			if (child != null)
				child = VisitNode(child);

			return new CachePointNode(child);
		}

		protected virtual IQueryPlanNode VisitDistinct(DistinctNode node) {
			var child = node.Child;
			if (child != null)
				child = VisitNode(child);

			return new DistinctNode(child, node.ColumnNames);
		}

		protected virtual IQueryPlanNode VisitConstantSelect(ConstantSelectNode node) {
			var child = node.Child;
			if (child != null)
				child = VisitNode(child);

			return new ConstantSelectNode(child, node.Expression);
		}

		protected virtual IQueryPlanNode VisitExhaustiveSelect(ExhaustiveSelectNode node) {
			var child = node.Child;
			if (child != null)
				child = VisitNode(child);

			return new ExhaustiveSelectNode(child, node.Expression);
		}

		protected virtual IQueryPlanNode VisitSimpleSelect(SimpleSelectNode node) {
			var child = node.Child;
			if (child != null)
				child = VisitNode(child);

			return new SimpleSelectNode(child, node.LeftColumnName, node.OperatorType, node.RightExpression);
		}
	}
}