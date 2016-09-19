using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Deveel.Data.Design;
using Deveel.Data.Sql.Statements;

using Remotion.Linq;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Parsing;

namespace Deveel.Data.Linq {
	class SqlQueryGenerator : QueryModelVisitorBase {
		private ExpressionCompileContext buildContext;

		private SqlQueryGenerator(IQuery query) {
			var model = query.CompileModel();
			buildContext = new ExpressionCompileContext(model);
		}

		public static SelectStatement GenerateSqlQuery(IQuery query, QueryModel queryModel) {
			var visitor = new SqlQueryGenerator(query);
			visitor.VisitQueryModel(queryModel);
			return visitor.GetQuery();
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
			VisitBodyClauses(queryModel.BodyClauses, queryModel);
		}

		public SelectStatement GetQuery() {
			return buildContext.BuildQueryExpression();
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
