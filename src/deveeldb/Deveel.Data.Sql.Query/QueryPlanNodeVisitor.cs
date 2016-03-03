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
			if (node is SingleRowTableNode)
				return VisitSingleRowTable((SingleRowTableNode) node);

			throw new NotSupportedException();
		}

		protected virtual IQueryPlanNode VisitSingleRowTable(SingleRowTableNode node) {
			return new SingleRowTableNode();
		}

		protected virtual IQueryPlanNode VisitFetchView(FetchViewNode node) {
			return new FetchViewNode(node.ViewName, node.AliasName);
		}

		protected virtual IQueryPlanNode VisitFetchTable(FetchTableNode node) {
			return new FetchTableNode(node.TableName, node.AliasName);
		}

		protected virtual IQueryPlanNode VisitSingle(SingleQueryPlanNode node) {
			if (node is SimpleSelectNode)
				return VisitSimpleSelect((SimpleSelectNode) node);
			if (node is SimplePatternSelectNode)
				return VisitSimplePatternSelect((SimplePatternSelectNode) node);
			if (node is ExhaustiveSelectNode)
				return VisitExhaustiveSelect((ExhaustiveSelectNode) node);
			if (node is ConstantSelectNode)
				return VisitConstantSelect((ConstantSelectNode) node);
			if (node is RangeSelectNode)
				return VisitRangeSelect((RangeSelectNode) node);
			if (node is DistinctNode)
				return VisitDistinct((DistinctNode) node);
			if (node is LeftOuterJoinNode)
				return VisitLeftOuterJoin((LeftOuterJoinNode) node);
			if (node is CachePointNode)
				return VisitCachePoint((CachePointNode) node);
			if (node is MarkerNode)
				return VisitMarker((MarkerNode) node);
			if (node is SubsetNode)
				return VisitSubset((SubsetNode) node);
			if (node is GroupNode)
				return VisitGroup((GroupNode) node);
			if (node is SortNode)
				return VisitSort((SortNode) node);
			if (node is CreateFunctionsNode)
				return VisitCreateFunctions((CreateFunctionsNode) node);
			if (node is LimitNode)
				return VisitLimit((LimitNode) node);

			throw new NotSupportedException();
		}

		protected virtual IQueryPlanNode VisitLimit(LimitNode node) {
			var child = node.Child;
			if (child != null)
				child = VisitNode(child);

			return new LimitNode(child, node.Offset, node.Count);
		}

		protected virtual IQueryPlanNode VisitCreateFunctions(CreateFunctionsNode node) {
			var child = node.Child;
			if (child != null)
				child = VisitNode(child);

			return new CreateFunctionsNode(child, node.Functions, node.Names);
		}

		protected virtual IQueryPlanNode VisitBranch(BranchQueryPlanNode node) {
			if (node is CompositeNode)
				return VisitComposite((CompositeNode) node);
			if (node is JoinNode)
				return VisitJoin((JoinNode) node);
			if (node is EquiJoinNode)
				return VisitEquiJoin((EquiJoinNode) node);
			if (node is NaturalJoinNode)
				return VisitNaturalJoin((NaturalJoinNode) node);
			if (node is LogicalUnionNode)
				return VisitLogicalUnion((LogicalUnionNode) node);
			if (node is NonCorrelatedAnyAllNode)
				return VisitNonCorrelatedAnyAll((NonCorrelatedAnyAllNode) node);

			throw new NotSupportedException();
		}

		protected virtual IQueryPlanNode VisitNonCorrelatedAnyAll(NonCorrelatedAnyAllNode node) {
			var left = node.Left;
			var right = node.Right;
			if (left != null)
				left = VisitNode(left);
			if (right != null)
				right = VisitNode(right);

			return new NonCorrelatedAnyAllNode(left, right, node.LeftColumnNames, node.SubQueryType);
		}

		protected virtual IQueryPlanNode VisitLogicalUnion(LogicalUnionNode node) {
			var left = node.Left;
			var right = node.Right;
			if (left != null)
				left = VisitNode(left);
			if (right != null)
				right = VisitNode(right);

			return new LogicalUnionNode(left, right);
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

		protected virtual IQueryPlanNode VisitSimplePatternSelect(SimplePatternSelectNode node) {
			var child = node.Child;
			if (child != null)
				child = VisitNode(child);

			return new SimplePatternSelectNode(child, node.Expression);
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

		protected virtual IQueryPlanNode VisitSort(SortNode node) {
			var child = node.Child;
			if (child != null)
				child = VisitNode(child);

			return new SortNode(child, node.ColumnNames, node.Ascending);
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

			return new SimpleSelectNode(child, node.ColumnName, node.OperatorType, node.Expression);
		}

		protected virtual IQueryPlanNode VisitSubset(SubsetNode node) {
			var child = node.Child;
			if (child != null)
				child = VisitNode(child);

			return new SubsetNode(child, node.OriginalColumnNames, node.AliasColumnNames);
		}
	}
}