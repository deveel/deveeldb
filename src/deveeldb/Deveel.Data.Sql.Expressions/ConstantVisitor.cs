using System;

using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Expressions {
	class ConstantVisitor : SqlExpressionVisitor {
		public bool IsConstant { get; private set; }

		public override SqlExpression VisitConstant(SqlConstantExpression constant) {
			var value = constant.Value;
			if (value.Type.SqlType == SqlTypeCode.Array) {
				var array = value.Value as SqlArray;
				if (array != null && !array.IsNull) {
					foreach (var exp in array) {
						if (!exp.IsConstant()) {
							IsConstant = false;
							break;
						}
					}
				}
			}

			return base.VisitConstant(constant);
		}

		public override SqlExpression VisitReference(SqlReferenceExpression reference) {
			return base.VisitReference(reference);
		}

		public override SqlExpression VisitFunctionCall(SqlFunctionCallExpression expression) {
			return base.VisitFunctionCall(expression);
		}

		public override SqlExpression VisitQuery(SqlQueryExpression query) {
			return base.VisitQuery(query);
		}
	}
}