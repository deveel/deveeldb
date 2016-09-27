using System;
using System.Linq.Expressions;
using System.Text;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Parsing;

namespace Deveel.Data.Linq {
	class ExpressionToSqlExpressionVisitor : RelinqExpressionVisitor {
		public static SqlExpression GetSqlExpression(ExpressionCompileContext context, Expression expression) {
			var visitor = new ExpressionToSqlExpressionVisitor(context);
			visitor.Visit(expression);
			return visitor.Result;
		}

		private SqlExpression GetSqlExpression(Expression expression) {
			return GetSqlExpression(Context, expression);
		}

		private ExpressionToSqlExpressionVisitor(ExpressionCompileContext context) {
			Context = context;
		}

		private ExpressionCompileContext Context { get; set; }

		private SqlExpression Result { get; set; }

		private static SqlExpressionType GetBinaryOperator(ExpressionType nodeType) {
			switch (nodeType) {
				case ExpressionType.Add:
				case ExpressionType.AddChecked:
					return SqlExpressionType.Add;
				case ExpressionType.Subtract:
				case ExpressionType.SubtractChecked:
					return SqlExpressionType.Subtract;
				case ExpressionType.Multiply:
				case ExpressionType.MultiplyChecked:
					return SqlExpressionType.Multiply;
				case ExpressionType.Divide:
					return SqlExpressionType.Divide;
				case ExpressionType.Modulo:
					return SqlExpressionType.Modulo;
				case ExpressionType.Equal:
					return SqlExpressionType.Equal;
				case ExpressionType.NotEqual:
					return SqlExpressionType.NotEqual;
				case ExpressionType.GreaterThan:
					return SqlExpressionType.GreaterThan;
				case ExpressionType.GreaterThanOrEqual:
					return SqlExpressionType.GreaterOrEqualThan;
				case ExpressionType.LessThan:
					return SqlExpressionType.SmallerThan;
				case ExpressionType.LessThanOrEqual:
					return SqlExpressionType.SmallerOrEqualThan;
				case ExpressionType.And:
					return SqlExpressionType.And;
				case ExpressionType.Or:
					return SqlExpressionType.Or;
				case ExpressionType.ExclusiveOr:
					return SqlExpressionType.XOr;
				default:
					throw new NotSupportedException();
			}
		}

		private static SqlExpressionType GetUnaryOperator(ExpressionType nodeType) {
			switch (nodeType) {
				case ExpressionType.UnaryPlus:
					return SqlExpressionType.UnaryPlus;
				case ExpressionType.Negate:
				case ExpressionType.NegateChecked:
					return SqlExpressionType.Negate;
				case ExpressionType.Not:
					return SqlExpressionType.Not;
				default:
					throw new NotSupportedException();
			}
		}

		protected override Expression VisitBinary(BinaryExpression b) {
			var left = GetSqlExpression(b.Left);
			var right = GetSqlExpression(b.Right);
			var op = GetBinaryOperator(b.NodeType);

			Result = SqlExpression.Binary(left, op, right);

			return b;
		}

		protected override Expression VisitConstant(ConstantExpression c) {
			var value = Field.Create(c.Value);
			var paramName = Context.AddParameter(value);

			Result = SqlExpression.VariableReference(paramName);

			return c;
		}

		protected override Expression VisitMethodCall(MethodCallExpression expression) {
			var methodName = expression.Method.Name;
			var type = expression.Method.DeclaringType;

			if (methodName == "Equals") {
				var left = GetSqlExpression(expression.Object);
				var right = ((ConstantExpression) expression.Arguments[0]).Value;

				Result = SqlExpression.Equal(left, SqlExpression.Constant(right));
			} else if (methodName == "ToString") {
				var left = GetSqlExpression(expression.Object);
				var castType = PrimitiveTypes.String();

				Result = SqlExpression.Cast(left, castType);
			} else if (type == typeof(string)) {
				switch (methodName) {
					case "StartsWith": {
						if (expression.Arguments.Count > 1)
							throw new NotSupportedException();

						var left = GetSqlExpression(expression.Object);
						var like = (string) ((ConstantExpression) expression.Arguments[0]).Value;
						var s = String.Format("{0}%", like);

						Result = SqlExpression.Like(left, SqlExpression.Constant(s));
						break;
					}
					case "Contains": {
						if (expression.Arguments.Count > 1)
							throw new NotSupportedException();

						var left = GetSqlExpression(expression.Object);
						var like = (string) ((ConstantExpression) expression.Arguments[0]).Value;
						var s = String.Format("%{0}%", like);

						Result = SqlExpression.Like(left, SqlExpression.Constant(s));
						break;
					}
					case "EndsWith": {
						if (expression.Arguments.Count > 1)
							throw new NotSupportedException();

						var left = GetSqlExpression(expression.Object);
						var like = (string) ((ConstantExpression) expression.Arguments[0]).Value;

						var s = String.Format("%{0}", like);

						Result = SqlExpression.Like(left, SqlExpression.Constant(s));

						break;
					}
					// TODO:
					//case "Trim": {
					//	var left = GetSqlExpression(expression.Object);
					//	char[] chars = null;
					//	if (expression.Arguments.Count > 0)
					//		chars = (char[]) ((ConstantExpression) expression.Arguments[0]).Value;

					//	if (chars == null) {
					//		Result = SqlExpression.FunctionCall("TRIM", new SqlExpression[] {left});
					//	} else {
					//		var s = SqlExpression.Constant(Field.String(new string(chars)));
					//		Result = SqlExpression.FunctionCall("TRIM", new SqlExpression[] {left, s});
					//	}

					//	break;
					//}
					//case "TrimStart": {
					//	break;
					//}
					//case "TrimEnd": {
					//	break;
					//}
					default:
						throw new NotSupportedException();
				}
			} else if (type == typeof(DateTime) ||
			           type == typeof(DateTimeOffset)) {
				throw new NotImplementedException();
			}

			return expression;
		}

		private string lastSource;

		protected override Expression VisitQuerySourceReference(QuerySourceReferenceExpression expression) {
			var source = expression.ReferencedQuerySource;
			var tableName = Context.FindTableName(source.ItemType);

			lastSource = tableName;

			return expression;
		}

		protected override Expression VisitMember(MemberExpression expression) {
			var type = expression.Member.ReflectedType;
			var memberName = expression.Member.Name;

			var mapInfo = Context.GetMemberMap(type, memberName);

			var columnName = new ObjectName(mapInfo.ColumnName);

			if (!String.IsNullOrEmpty(lastSource))
				columnName = new ObjectName(new ObjectName(lastSource), columnName.FullName);

			Result = SqlExpression.Reference(columnName);

			return expression;
		}

		protected override Expression VisitUnary(UnaryExpression u) {
			var op = GetUnaryOperator(u.NodeType);
			var operand = GetSqlExpression(u.Operand);

			Result = SqlExpression.Unary(op, operand);

			return u;
		}
	}
}
