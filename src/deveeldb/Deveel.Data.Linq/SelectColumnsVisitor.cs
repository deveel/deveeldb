using System;
using System.Linq.Expressions;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

using Remotion.Linq;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.ResultOperators;

namespace Deveel.Data.Linq {
	class SelectColumnsVisitor : QueryModelVisitorBase {
		public SelectColumnsVisitor(ExpressionCompileContext context) {
			Context = context;
		}

		public ExpressionCompileContext Context { get; private set; }

		public override void VisitSelectClause(SelectClause selectClause, QueryModel queryModel) {
			var visitor = new SqlSelectExpressionVisitor(Context);
			visitor.Visit(selectClause.Selector);

			base.VisitSelectClause(selectClause, queryModel);
		}

		public override void VisitResultOperator(ResultOperatorBase resultOperator, QueryModel queryModel, int index) {
			bool handled = true;

			if (resultOperator is CountResultOperator) {
				if (Context.Columns.Count > 0) {
					var column = Context.Columns[index];
					var expression = column.Expression;


					bool take = true;
					if (expression is SqlFunctionCallExpression) {
						var func = (SqlFunctionCallExpression) expression;
						if (func.FunctioName.Equals(new ObjectName("COUNT"), true) &&
						    func.Arguments.Length == 1 &&
						    func.Arguments[0].Value is SqlConstantExpression &&
						    ((SqlConstantExpression) func.Arguments[0].Value).Value.Type is StringType &&
						    ((SqlString) ((SqlConstantExpression) func.Arguments[0].Value).Value.Value) == "*") {
							take = false;
						}
					}

					if (take) {
						expression = SqlExpression.FunctionCall("COUNT", new[] {expression});
					}

					Context.Columns[index] = new SelectColumn(expression, column.Alias);
				} else {
					var expression = SqlExpression.FunctionCall("COUNT", new[] {SqlExpression.Constant("*")});
					Context.Columns.Add(new SelectColumn(expression));
				}
			} else if (resultOperator is MaxResultOperator) {
				var column = Context.Columns[index];
				var expression = column.Expression;

				expression = SqlExpression.FunctionCall("MAX", new [] {expression});
				Context.Columns[index] = new SelectColumn(expression);
			} else if (resultOperator is MinResultOperator) {
				var column = Context.Columns[index];
				var expression = column.Expression;

				expression = SqlExpression.FunctionCall("MIN", new[] { expression });
				Context.Columns[index] = new SelectColumn(expression);
			} else if (resultOperator is AverageResultOperator) {
				var column = Context.Columns[index];
				var expression = column.Expression;

				expression = SqlExpression.FunctionCall("AVG", new[] { expression });
				Context.Columns[index] = new SelectColumn(expression);
			} else if (resultOperator is SumResultOperator) {
				var column = Context.Columns[index];
				var expression = column.Expression;

				expression = SqlExpression.FunctionCall("SUM", new[] { expression });
				Context.Columns[index] = new SelectColumn(expression);
			} else if (resultOperator is FirstResultOperator) {
				Context.Limit(1);
				Context.StartAt(0);
			} else if (resultOperator is TakeResultOperator) {
				var take = (TakeResultOperator) resultOperator;
				int count;
				if (take.Count is ConstantExpression) {
					count = take.GetConstantCount(); // TODO: support also non-constant
				} else {
					throw new NotSupportedException();
				}

				Context.Limit(count);
			} else if (resultOperator is SkipResultOperator) {
				var skip = (SkipResultOperator) resultOperator;
				var count = skip.GetConstantCount(); // TODO: support also non-constant
				Context.StartAt(count);
			} else if (resultOperator is SingleResultOperator) {
				Context.Limit(1);
			} else if (resultOperator is AnyResultOperator) {
				// TODO: make a subquery and count that the result is at least 1
				var query = Context.BuildQueryExpression();
				const string queryRef = "q1";
				Context.Clear();
				Context.AddQuerySource(query, queryRef);
				Context.Columns.Add(
					new SelectColumn(
						SqlExpression.GreaterOrEqualThan(SqlExpression.FunctionCall("COUNT", new[] {SqlExpression.Constant(queryRef)}),
							SqlExpression.Constant(1))));
			} else if (resultOperator is AllResultOperator) {
				// TODO:
				handled = false;
			} else {
				handled = false;
			}

			if (!handled)
				throw new NotSupportedException();

			base.VisitResultOperator(resultOperator, queryModel, index);
		}
	}
}
