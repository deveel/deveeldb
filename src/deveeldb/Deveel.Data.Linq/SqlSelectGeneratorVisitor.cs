using System;
using System.Linq.Expressions;

using Deveel.Data.Design;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Variables;

using Remotion.Linq;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Parsing;

namespace Deveel.Data.Linq {
	class SqlSelectGeneratorVisitor : QueryModelVisitorBase {
		private ExpressionCompileContext buildContext;

		private SqlSelectGeneratorVisitor(DbCompiledModel model) {
			buildContext = new ExpressionCompileContext(model);
		}

		public static SelectStatement GenerateSelect(IQuery context, DbCompiledModel model, QueryModel queryModel) {
			var visitor = new SqlSelectGeneratorVisitor(model);
			visitor.VisitQueryModel(queryModel);
			return visitor.GetSelect(context);
		}

		private void DiscoverSources(MainFromClause fromClause) {
			var visitor = new SourceDiscoveryVisitor(buildContext);
			visitor.Visit(fromClause.FromExpression);

			buildContext.AddTypeSource(fromClause.ItemType);
			buildContext.AddAlias(fromClause.ItemType, fromClause.ItemName, "t0");
		}

		public override void VisitQueryModel(QueryModel queryModel) {
			DiscoverSources(queryModel.MainFromClause);

			queryModel.MainFromClause.Accept(this, queryModel);
			queryModel.Accept(new WhereQueryVisitor(buildContext));
			queryModel.Accept(new SelectColumnsVisitor(buildContext));
			queryModel.Accept(new OrderByVisitor(buildContext));
			VisitResultOperators(queryModel.ResultOperators, queryModel);
			VisitBodyClauses(queryModel.BodyClauses, queryModel);
		}

		public SelectStatement GetSelect(IQuery context) {
			var statement = buildContext.BuildSelectStatement();
			if (buildContext.Parameters.Count > 0) {
				foreach (var parameter in buildContext.Parameters) {
					context.Context.SetVariable(parameter.Key, parameter.Value);
				}
			}

			return statement;
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
				context.AddTypeSource(source.ItemType);
				var name = String.Format("t{0}", ++sourceCount);

				context.AddAlias(source.ItemType, source.ItemName, name);

				return expression;
			}
		}

		#endregion
	}
}
