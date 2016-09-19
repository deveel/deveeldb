using System;

using Remotion.Linq;
using Remotion.Linq.Clauses;
using Remotion.Linq.Parsing;

namespace Deveel.Data.Linq {
	class WhereQueryVisitor : QueryModelVisitorBase {
		public WhereQueryVisitor(ExpressionCompileContext context) {
			Context = context;
		}

		public ExpressionCompileContext Context { get; private set; }

		public override void VisitWhereClause(WhereClause whereClause, QueryModel queryModel, int index) {
			var sqlExpression = ExpressionToSqlExpressionVisitor.GetSqlExpression(Context, whereClause.Predicate);

			Context.SetFilter(sqlExpression);

			base.VisitWhereClause(whereClause, queryModel, index);
		}
	}
}
