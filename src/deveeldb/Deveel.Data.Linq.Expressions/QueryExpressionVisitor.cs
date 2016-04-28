using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Linq.Expressions {
	public class QueryExpressionVisitor : ExpressionVisitor {
		protected override Expression Visit(Expression exp) {
			if (exp == null)
				return null;

			switch ((QueryExpressionType) exp.NodeType) {
				case QueryExpressionType.Table:
					return VisitTable((TableExpression) exp);
				case QueryExpressionType.Column:
					return VisitColumn((ColumnExpression) exp);
				case QueryExpressionType.Select:
					return VisitSelect((SelectExpression) exp);
				case QueryExpressionType.Join:
					return VisitJoin((JoinExpression) exp);
				case QueryExpressionType.Aggregate:
					return VisitAggregate((AggregateExpression) exp);
				case QueryExpressionType.AggregateSubquery:
					return VisitAggregateSubquery((AggregateSubqueryExpression)exp);
				case QueryExpressionType.Scalar:
				case QueryExpressionType.In:
				case QueryExpressionType.Exists:
					return VisitSubquery((SubqueryExpression) exp);
				case QueryExpressionType.IsNull:
					return VisitIsNull((IsNullExpression) exp);
				case QueryExpressionType.Between:
					return VisitBetween((BetweenExpression) exp);
				case QueryExpressionType.Projection:
					return VisitProjection((ProjectionExpression) exp);
				case QueryExpressionType.Variable:
					return VisitVariable((VariableExpression) exp);
				case QueryExpressionType.Function:
					return VisitFunction((FunctionExpression) exp);
				case QueryExpressionType.Entity:
					return VisitEntity((EntityExpression) exp);
				default:
					return base.Visit(exp);
			}
		}

		protected virtual Expression VisitAggregateSubquery(AggregateSubqueryExpression aggregate) {
			var subquery = (ScalarExpression)Visit(aggregate.AsSubquery);
			return UpdateAggregateSubquery(aggregate, subquery);
		}

		protected AggregateSubqueryExpression UpdateAggregateSubquery(AggregateSubqueryExpression aggregate, ScalarExpression subquery) {
			if (subquery != aggregate.AsSubquery) {
				return new AggregateSubqueryExpression(aggregate.GroupBy, aggregate.InGroupSelect, subquery);
			}
			return aggregate;
		}

		protected virtual Expression VisitSource(Expression source) {
			return this.Visit(source);
		}

		protected virtual Expression VisitProjection(ProjectionExpression proj) {
			var select = (SelectExpression)this.Visit(proj.Source);
			var projector = this.Visit(proj.Projector);
			return this.UpdateProjection(proj, select, projector, proj.Aggregate);
		}

		protected ProjectionExpression UpdateProjection(ProjectionExpression proj, SelectExpression select, Expression projector, LambdaExpression aggregator) {
			if (select != proj.Source || projector != proj.Projector || aggregator != proj.Aggregate) {
				return new ProjectionExpression(select, projector, aggregator);
			}
			return proj;
		}

		protected virtual Expression VisitEntity(EntityExpression entity) {
			var exp = this.Visit(entity.Expression);
			return this.UpdateEntity(entity, exp);
		}

		protected EntityExpression UpdateEntity(EntityExpression entity, Expression expression) {
			if (expression != entity.Expression) {
				return new EntityExpression(entity.Entity, expression);
			}
			return entity;
		}

		protected virtual Expression VisitTable(TableExpression table) {
			return table;
		}

		protected virtual Expression VisitColumn(ColumnExpression column) {
			return column;
		}

		protected virtual Expression VisitSelect(SelectExpression select) {
			var from = this.VisitSource(select.From);
			var where = this.Visit(select.Where);
			var orderBy = this.VisitOrderBy(select.OrderBy);
			var groupBy = this.VisitExpressionList(select.GroupBy);
			var skip = this.Visit(select.Skip);
			var take = this.Visit(select.Take);
			var columns = this.VisitQueryColumns(select.Columns);
			return this.UpdateSelect(select, from, where, orderBy, groupBy, skip, take, select.Distinct, columns);
		}

		protected SelectExpression UpdateSelect(
			SelectExpression select,
			Expression from, Expression where,
			IEnumerable<OrderExpression> orderBy, IEnumerable<Expression> groupBy,
			Expression skip, Expression take,
			bool isDistinct,
			IEnumerable<QueryColumn> columns
			) {
			if (from != select.From
			    || where != select.Where
			    || orderBy != select.OrderBy
			    || groupBy != select.GroupBy
			    || take != select.Take
			    || skip != select.Skip
			    || isDistinct != select.Distinct
			    || columns != select.Columns
				) {
				return new SelectExpression(isDistinct, columns, from, where, orderBy, groupBy, skip, take, select.Alias);
			}
			return select;
		}

		protected virtual ReadOnlyCollection<QueryColumn> VisitQueryColumns(ReadOnlyCollection<QueryColumn> columns) {
			List<QueryColumn> alternate = null;
			for (int i = 0, n = columns.Count; i < n; i++) {
				QueryColumn column = columns[i];
				Expression e = this.Visit(column.Expression);
				if (alternate == null && e != column.Expression) {
					alternate = columns.Take(i).ToList();
				}
				if (alternate != null) {
					alternate.Add(new QueryColumn(column.Name, e, column.Type));
				}
			}
			if (alternate != null) {
				return alternate.AsReadOnly();
			}
			return columns;
		}

		protected virtual Expression VisitJoin(JoinExpression join) {
			var left = this.VisitSource(join.Left);
			var right = this.VisitSource(join.Right);
			var condition = this.Visit(join.Condition);
			return this.UpdateJoin(join, join.JoinType, left, right, condition);
		}

		protected JoinExpression UpdateJoin(JoinExpression join, JoinType joinType, Expression left, Expression right,
			Expression condition) {
			if (joinType != join.JoinType || left != join.Left || right != join.Right || condition != join.Condition) {
				return new JoinExpression(left, joinType, right, condition);
			}
			return join;
		}

		protected virtual Expression VisitAggregate(AggregateExpression aggregate) {
			var arg = this.Visit(aggregate.Argument);
			return this.UpdateAggregate(aggregate, aggregate.Type, aggregate.AggregateName, arg, aggregate.Distinct);
		}

		protected AggregateExpression UpdateAggregate(AggregateExpression aggregate, Type type, string aggType, Expression arg, bool isDistinct) {
			if (type != aggregate.Type || aggType != aggregate.AggregateName || arg != aggregate.Argument || isDistinct != aggregate.Distinct) {
				return new AggregateExpression(aggType, arg, isDistinct, type);
			}
			return aggregate;
		}

		protected virtual Expression VisitIsNull(IsNullExpression isnull) {
			var expr = this.Visit(isnull.Expression);
			return this.UpdateIsNull(isnull, expr);
		}

		protected IsNullExpression UpdateIsNull(IsNullExpression isnull, Expression expression) {
			if (expression != isnull.Expression) {
				return new IsNullExpression(expression);
			}
			return isnull;
		}

		protected virtual Expression VisitBetween(BetweenExpression between) {
			var expr = this.Visit(between.Expression);
			var lower = this.Visit(between.Lower);
			var upper = this.Visit(between.Upper);
			return this.UpdateBetween(between, expr, lower, upper);
		}

		protected BetweenExpression UpdateBetween(BetweenExpression between, Expression expression, Expression lower, Expression upper) {
			if (expression != between.Expression || lower != between.Lower || upper != between.Upper) {
				return new BetweenExpression(expression, lower, upper);
			}
			return between;
		}

		protected virtual Expression VisitSubquery(SubqueryExpression subquery) {
			switch ((QueryExpressionType)subquery.NodeType) {
				case QueryExpressionType.Scalar:
					return this.VisitScalar((ScalarExpression)subquery);
				case QueryExpressionType.Exists:
					return this.VisitExists((ExistsExpression)subquery);
				case QueryExpressionType.In:
					return this.VisitIn((InExpression)subquery);
			}
			return subquery;
		}

		protected virtual Expression VisitScalar(ScalarExpression scalar) {
			var select = (SelectExpression)this.Visit(scalar.Query);
			return this.UpdateScalar(scalar, select);
		}

		protected ScalarExpression UpdateScalar(ScalarExpression scalar, SelectExpression select) {
			if (select != scalar.Query) {
				return new ScalarExpression(scalar.Type, select);
			}
			return scalar;
		}

		protected virtual Expression VisitExists(ExistsExpression exists) {
			var select = (SelectExpression)this.Visit(exists.Query);
			return this.UpdateExists(exists, select);
		}

		protected ExistsExpression UpdateExists(ExistsExpression exists, SelectExpression select) {
			if (select != exists.Query) {
				return new ExistsExpression(select);
			}
			return exists;
		}

		protected virtual Expression VisitIn(InExpression @in) {
			var expr = this.Visit(@in.Expression);
			var select = (SelectExpression)this.Visit(@in.Query);
			var values = this.VisitExpressionList(@in.Values);
			return this.UpdateIn(@in, expr, select, values);
		}

		protected InExpression UpdateIn(InExpression @in, Expression expression, SelectExpression select,
			IEnumerable<Expression> values) {
			if (expression != @in.Expression || select != @in.Query || values != @in.Values) {
				if (select != null) {
					return new InExpression(expression, select);
				} else {
					return new InExpression(expression, values);
				}
			}
			return @in;
		}

		protected virtual Expression VisitBlock(BlockExpression block) {
			var commands = this.VisitExpressionList(block.Expressions);
			return this.UpdateBlock(block, commands);
		}

		protected BlockExpression UpdateBlock(BlockExpression block, IList<Expression> expressions) {
			if (block.Expressions != expressions) {
				return new BlockExpression(expressions);
			}
			return block;
		}

		protected virtual Expression VisitVariable(VariableExpression vex) {
			return vex;
		}

		protected virtual Expression VisitFunction(FunctionExpression func) {
			var arguments = this.VisitExpressionList(func.Arguments);
			return this.UpdateFunction(func, func.FunctionName, arguments);
		}

		protected FunctionExpression UpdateFunction(FunctionExpression func, string name, IEnumerable<Expression> arguments) {
			if (name != func.FunctionName || arguments != func.Arguments) {
				return new FunctionExpression(func.Type, name, arguments);
			}
			return func;
		}

		protected virtual ReadOnlyCollection<OrderExpression> VisitOrderBy(ReadOnlyCollection<OrderExpression> expressions) {
			if (expressions != null) {
				List<OrderExpression> alternate = null;
				for (int i = 0, n = expressions.Count; i < n; i++) {
					OrderExpression expr = expressions[i];
					Expression e = this.Visit(expr.Expression);
					if (alternate == null && e != expr.Expression) {
						alternate = expressions.Take(i).ToList();
					}
					if (alternate != null) {
						alternate.Add(new OrderExpression(e, expr.OrderType));
					}
				}
				if (alternate != null) {
					return alternate.AsReadOnly();
				}
			}
			return expressions;
		}
	}
}