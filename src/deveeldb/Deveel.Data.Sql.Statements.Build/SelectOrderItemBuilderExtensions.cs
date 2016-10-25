using System;

using Deveel.Data.Sql.Expressions.Build;

namespace Deveel.Data.Sql.Statements.Build {
	public static class SelectOrderItemBuilderExtensions {
		public static ISelectOrderBuilder Descending(this ISelectOrderBuilder builder) {
			return builder.Direction(SortDirection.Descending);
		}

		public static ISelectOrderBuilder Ascending(this ISelectOrderBuilder builder) {
			return builder.Direction(SortDirection.Ascending);
		}

		public static ISelectOrderBuilder Expression(this ISelectOrderBuilder builder, Action<IExpressionBuilder> expression) {
			var expBuilder = new ExpressionBuilder();
			expression(expBuilder);

			return builder.Expression(expBuilder.Build());
		}

		public static ISelectOrderBuilder Column(this ISelectOrderBuilder builder, ObjectName columnName) {
			return builder.Expression(expression => expression.Reference(columnName));
		}

		public static ISelectOrderBuilder Column(this ISelectOrderBuilder builder, ObjectName parentName, string name) {
			return builder.Column(new ObjectName(parentName, name));
		}

		public static ISelectOrderBuilder Column(this ISelectOrderBuilder builder, string parentName, string name) {
			return builder.Column(ObjectName.Parse(parentName), name);
		}
	}
}
