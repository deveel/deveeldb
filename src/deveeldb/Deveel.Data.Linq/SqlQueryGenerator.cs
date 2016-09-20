using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Deveel.Data.Design;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Variables;

using Remotion.Linq;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Parsing;

namespace Deveel.Data.Linq {
	class SqlQueryGenerator : QueryModelVisitorBase {
		private ExpressionCompileContext buildContext;

		private SqlQueryGenerator(DbCompiledModel model) {
			buildContext = new ExpressionCompileContext(model);
		}

		public static SelectStatement GenerateSelect(IQuery context, DbCompiledModel model, QueryModel queryModel) {
			var visitor = new SqlQueryGenerator(model);
			visitor.VisitQueryModel(queryModel);
			return visitor.GetQuery(context);
		}

		private void DiscoverSources(MainFromClause fromClause) {
			var visitor = new SourceDiscoveryVisitor(buildContext);
			visitor.Visit(fromClause.FromExpression);

			buildContext.AddSource(fromClause.ItemType);
			buildContext.AddAlias(fromClause.ItemType, fromClause.ItemName, "t0");
		}

		public override void VisitQueryModel(QueryModel queryModel) {
			DiscoverSources(queryModel.MainFromClause);

			queryModel.SelectClause.Accept(new SelectColumnsVisitor(buildContext), queryModel);
			queryModel.MainFromClause.Accept(this, queryModel);
			queryModel.Accept(new OrderByVisitor(buildContext));
			queryModel.Accept(new WhereQueryVisitor(buildContext));
			VisitResultOperators(queryModel.ResultOperators, queryModel);
			VisitBodyClauses(queryModel.BodyClauses, queryModel);
		}

		public SelectStatement GetQuery(IQuery context) {
			var statement = buildContext.BuildQueryExpression();
			if (buildContext.Parameters.Count > 0) {
				foreach (var parameter in buildContext.Parameters) {
					context.Context.SetVariable(parameter.Key, parameter.Value);
				}
			}

			return statement;
		}

		public override void VisitResultOperator(ResultOperatorBase resultOperator, QueryModel queryModel, int index) {
			resultOperator.Accept(new SelectColumnsVisitor(buildContext), queryModel, index);

			base.VisitResultOperator(resultOperator, queryModel, index);
		}

		#region SourceDiscoveryVisitor

		class SourceDiscoveryVisitor : RelinqExpressionVisitor {
			private int sourceCount = 0;
			private ExpressionCompileContext context;

			public SourceDiscoveryVisitor(ExpressionCompileContext context) {
				this.context = context;
			}

			protected override Expression VisitQuerySourceReference(QuerySourceReferenceExpression expression) {
				var source = expression.ReferencedQuerySource;
				context.AddSource(source.ItemType);
				var name = String.Format("t{0}", ++sourceCount);

				context.AddAlias(source.ItemType, source.ItemName, name);

				return expression;
			}
		}

		#endregion
	}
}
