using System;
using System.Collections.Generic;
using System.Data.Common.CommandTrees;
using System.Data.Metadata.Edm;
using System.Runtime.Serialization;

using Deveel.Data.Sql;

namespace Deveel.Data.Entity {
	internal class SelectExpressionVisitor : ExpressionVisitor {
		Stack<SelectExpression> selectStatements = new Stack<SelectExpression>();

		private SelectExpression VisitInputExpressionEnsureSelect(DbExpression e, string name, TypeUsage type) {
			BranchExpression expression = VisitInputExpression(e, name, type);
			if (expression is SelectExpression) 
				return (expression as SelectExpression);

			SelectExpression s = new SelectExpression();

			// if the fragment is a union then it needs to be wrapped
			if (expression is CompositeExpression)
				(expression as CompositeExpression).InsertInScope(scope);

			s.From = expression;
			return s;
		}

		private SelectExpression WrapIfNotCompatible(SelectExpression select, DbExpressionKind expressionKind) {
			if (select.IsCompatible(expressionKind))
				return select;
			SelectExpression newSelect = new SelectExpression();
			select.InsertInScope(scope);
			newSelect.From = select;
			return newSelect;
		}

		private void WrapJoinInputIfNecessary(BranchExpression fragment, bool isRightPart) {
			if (fragment is SelectExpression ||
				fragment is CompositeExpression)
				fragment.InsertInScope(scope);
			else if (fragment is JoinExpression && isRightPart)
				fragment.InsertInScope(null);
		}



		public override Expression Visit(DbDistinctExpression expression) {
			SelectExpression select = VisitInputExpressionEnsureSelect(expression.Argument, null, null);
			select.IsDistinct = true;
			return select;
		}

		public override Expression Visit(DbFilterExpression expression) {
			SelectExpression select = VisitInputExpressionEnsureSelect(expression.Input.Expression,
				expression.Input.VariableName, expression.Input.VariableType);
			select = WrapIfNotCompatible(select, expression.ExpressionKind);
			select.Where = expression.Predicate.Accept(this);
			return select;
		}

		public override Expression Visit(DbGroupByExpression expression) {
			// first process the input
			DbGroupExpressionBinding e = expression.Input;
			SelectExpression innerSelect = VisitInputExpressionEnsureSelect(e.Expression, e.VariableName, e.VariableType);
			SelectExpression select = WrapIfNotCompatible(innerSelect, expression.ExpressionKind);

			CollectionType ct = (CollectionType)expression.ResultType.EdmType;
			RowType rt = (RowType)ct.TypeUsage.EdmType;

			int propIndex = 0;
			foreach (DbExpression key in expression.Keys) {
				select.AddGroupBy(key.Accept(this));
				propIndex++;
			}

			for (int agg = 0; agg < expression.Aggregates.Count; agg++) {
				DbAggregate a = expression.Aggregates[agg];
				DbFunctionAggregate fa = a as DbFunctionAggregate;
				if (fa == null) throw new NotSupportedException();

				string alias = rt.Properties[propIndex++].Name;
				VariableExpression functionCol = new VariableExpression();
				functionCol.Text = HandleFunction(fa, a.Arguments[0].Accept(this));
				functionCol.ColumnAlias = alias;
				select.Columns.Add(functionCol);
			}

			return select;
		}

		public override Expression Visit(DbProjectExpression expression) {
			SelectExpression select = VisitInputExpressionEnsureSelect(expression.Input.Expression,
				expression.Input.VariableName, expression.Input.VariableType);

			// see if we need to wrap this select inside a new select
			select = WrapIfNotCompatible(select, expression.ExpressionKind);

			if (!(expression.Projection is DbNewInstanceExpression))
				throw new ArgumentException();

			VisitNewInstanceExpression(select, expression.Projection as DbNewInstanceExpression);

			return select;
		}


		private Expression HandleFunction(DbFunctionAggregate fa, Expression arg) {
			if (fa.Arguments.Count != 1)
				throw new ArgumentException();

			if (fa.Function.NamespaceName != "Edm")
				throw new NotSupportedException();

			FunctionExpression fragment = new FunctionExpression();
			fragment.Name = fa.Function.Name;
			if (fa.Function.Name == "BigCount")
				fragment.Name = "Count";

			fragment.Distinct = fa.Distinct;
			fragment.Argmument = arg;
			return fragment;
		}

		public override Expression Visit(DbNewInstanceExpression expression) {
			if (!(expression.ResultType.EdmType is CollectionType))
				throw new ArgumentException();

			SelectExpression s = new SelectExpression();

			VariableExpression c = new VariableExpression();
			if (expression.Arguments.Count != 0)
				c.Text = expression.Arguments[0].Accept(this);
			else
				c.Text = new LiteralExpression("NULL");
			c.ColumnAlias = "X";
			s.Columns.Add(c);
			return s;
		}

		public override string BuildSqlStatement(DbCommandTree commandTree) {
			DbQueryCommandTree queryCommandTree = (DbQueryCommandTree) commandTree;

			DbExpression e = queryCommandTree.Query;
			if (e.ExpressionKind != DbExpressionKind.Project)
				throw new InvalidOperationException();

			Expression expression = e.Accept(this);
			if (!(expression is SelectExpression))
				throw new InvalidOperationException();

			return expression.ToString();
		}
	}
}