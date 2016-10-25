using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Expressions.Build;

namespace Deveel.Data.Sql.Statements.Build {
	public static class SelectStatementBuilderExtensions {
		public static ISelectStatementBuilder Query(this ISelectStatementBuilder builder, Action<IQueryExpressionBuilder> query) {
			var queryBuilder = new QueryExpressionBuilder();
			query(queryBuilder);

			return builder.Query(queryBuilder.Build());
		}

		public static ISelectStatementBuilder OrderBy(this ISelectStatementBuilder builder, params Action<ISelectOrderBuilder>[] sort) {
			foreach (var action in sort) {
				var sortBuilder = new SelectOrderBuilder();
				action(sortBuilder);

				builder = builder.OrderBy(sortBuilder.Build());
			}

			return builder;
		}

		public static ISelectStatementBuilder Limit(this ISelectStatementBuilder builder, Action<ISelectLimitBuilder> limit) {
			var limitBuilder = new SelectLimitBuilder();
			limit(limitBuilder);

			return builder.Limit(limitBuilder.Build());
		}

		#region SelectOrderBuilder

		class SelectOrderBuilder : ISelectOrderBuilder {
			private SqlExpression sqlExpression;
			private bool ascending;


			public ISelectOrderBuilder Expression(SqlExpression expression) {
				if (expression == null)
					throw new ArgumentNullException("expression");

				sqlExpression = expression;
				return this;
			}

			public ISelectOrderBuilder Direction(SortDirection direction) {
				ascending = (direction == SortDirection.Ascending);
				return this;
			}

			public SortColumn Build() {
				if (sqlExpression == null)
					throw new InvalidOperationException("The sort expression is required");

				return new SortColumn(sqlExpression, ascending);
			}
		}

		#endregion
	}
}
