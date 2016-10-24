using System;
using System.Linq;

namespace Deveel.Data.Sql.Expressions {
	public static class ExpressionBuilderExtensions {
		public static IExpressionBuilder Any(this IExpressionBuilder builder, Action<IExpressionBuilder> expression) {
			return builder.Quantified(SqlExpressionType.Any, expression);
		}

		public static IExpressionBuilder All(this IExpressionBuilder builder, Action<IExpressionBuilder> expression) {
			return builder.Quantified(SqlExpressionType.All, expression);
		}

		public static IExpressionBuilder In(this IExpressionBuilder builder, Action<IExpressionBuilder> expression) {
			return builder.Equal(eq => eq.Any(expression));
		}

		public static IExpressionBuilder NotIn(this IExpressionBuilder builder, Action<IExpressionBuilder> expression) {
			return builder.NotEqual(neq => neq.All(expression));
		}

		public static IExpressionBuilder And(this IExpressionBuilder builder, Action<IExpressionBuilder> right) {
			return builder.Binary(SqlExpressionType.And, right);
		}

		public static IExpressionBuilder Or(this IExpressionBuilder builder, Action<IExpressionBuilder> right) {
			return builder.Binary(SqlExpressionType.Or, right);
		}

		public static IExpressionBuilder XOr(this IExpressionBuilder builder, Action<IExpressionBuilder> right) {
			return builder.Binary(SqlExpressionType.XOr, right);
		}

		public static IExpressionBuilder Equal(this IExpressionBuilder builder, Action<IExpressionBuilder> right) {
			return builder.Binary(SqlExpressionType.Equal, right);
		}

		public static IExpressionBuilder NotEqual(this IExpressionBuilder builder, Action<IExpressionBuilder> right) {
			return builder.Binary(SqlExpressionType.NotEqual, right);
		}

		public static IExpressionBuilder Like(this IExpressionBuilder builder, Action<IExpressionBuilder> right) {
			return builder.Binary(SqlExpressionType.Like, right);
		}

		public static IExpressionBuilder Like(this IExpressionBuilder builder, string searchString) {
			return builder.Like(right => right.Value(searchString));
		}

		public static IExpressionBuilder Is(this IExpressionBuilder builder, Action<IExpressionBuilder> right) {
			return builder.Binary(SqlExpressionType.Is, right);
		}

		public static IExpressionBuilder IsNot(this IExpressionBuilder builder, Action<IExpressionBuilder> right) {
			return builder.Binary(SqlExpressionType.IsNot, right);
		}

		public static IExpressionBuilder Function(this IExpressionBuilder builder, string functionName,
			params SqlExpression[] args) {
			return builder.Function(ObjectName.Parse(functionName), args);
		}

		public static IExpressionBuilder Function(this IExpressionBuilder builder, ObjectName functionName, params Action<IExpressionBuilder>[] args) {
			return builder.Function(functionName, args.Select(x => {
				var expBuilder = new ExpressionBuilder();
				x(expBuilder);
				return expBuilder.Build();
			}).ToArray());
		}

		public static IExpressionBuilder Function(this IExpressionBuilder builder, string functionName,
			params Action<IExpressionBuilder>[] args) {
			return builder.Function(ObjectName.Parse(functionName), args);
		}

		public static IExpressionBuilder Function(this IExpressionBuilder builder, ObjectName functionName) {
			return builder.Function(functionName, new SqlExpression[0]);
		}

		public static IExpressionBuilder Function(this IExpressionBuilder builder, string functionName) {
			return builder.Function(functionName, new SqlExpression[0]);
		}

		public static IExpressionBuilder Reference(this IExpressionBuilder builder, string referenceName) {
			return builder.Reference(ObjectName.Parse(referenceName));
		}

		public static IExpressionBuilder Reference(this IExpressionBuilder builder, ObjectName parent, string name) {
			return builder.Reference(new ObjectName(parent, name));
		}

		public static IExpressionBuilder Reference(this IExpressionBuilder builder, string parentName, string name) {
			return builder.Reference(ObjectName.Parse(parentName), name);
		}
	}
}
