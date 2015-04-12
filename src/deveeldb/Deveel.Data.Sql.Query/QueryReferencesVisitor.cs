using System;
using System.Collections.Generic;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Sql.Query {
	class QueryReferencesVisitor : SqlExpressionVisitor {
		public QueryReferencesVisitor(IList<QueryReference> list, int level) {
			References = list;
			Level = level;
		}

		public IList<QueryReference> References { get; private set; }

		public int Level { get; private set; }

		public override SqlExpression Visit(SqlExpression expression) {
			if (expression is QueryReferenceExpression)
				VisitQueryReference((QueryReferenceExpression) expression);

			return base.Visit(expression);
		}

		private void VisitQueryReference(QueryReferenceExpression expression) {
				
		}

		public override SqlExpression VisitConstant(SqlConstantExpression constant) {
			var value = constant.Value;
			if (!value.IsNull && value.Value is SqlQueryObject &&
			    ((SqlQueryObject)value.Value).QueryPlan != null) {
				var queryObject = (SqlQueryObject)value.Value;
				var planNode = queryObject.QueryPlan;
				References = planNode.DiscoverQueryReferences(Level, References);
			}

			return base.VisitConstant(constant);
		}
	}
}