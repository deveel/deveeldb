using System;
using System.Collections.Generic;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Sql.Query {
	class TableNamesVisitor : SqlExpressionVisitor {
		public TableNamesVisitor(IList<ObjectName> tableNames) {
			TableNames = TableNames;
		}

		public IList<ObjectName> TableNames { get; private set; }

		public override SqlExpression VisitConstant(SqlConstantExpression constant) {
			var value = constant.Value;
			if (!value.IsNull && value.Value is SqlQueryObject &&
			    ((SqlQueryObject)value.Value).QueryPlan != null) {

				var queryObject = (SqlQueryObject) value.Value;
				var planNode = queryObject.QueryPlan;
				TableNames = planNode.DiscoverTableNames(TableNames);
			}

			return base.VisitConstant(constant);
		}
	}
}