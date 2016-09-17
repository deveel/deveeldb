using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

using Deveel.Data.Mapping;
using Deveel.Data.Routines;
using Deveel.Data.Sql;

using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Clauses.ExpressionVisitors;
using Remotion.Linq.Parsing;

namespace Deveel.Data.Linq {
	class SqlGeneratorExpressionVisitor : ThrowingExpressionVisitor {
		private readonly ICollection<QueryParameter> parameters;
		private readonly IDictionary<string, string> sources;
		private StringBuilder sql;

		public SqlGeneratorExpressionVisitor(IDictionary<string, string> sources, ICollection<QueryParameter> parameters) {
			this.sources = sources;
			sql = new StringBuilder();
			this.parameters = parameters;
		}

		public static string GetSqlExpression(Expression linqExpression, IDictionary<string, string> sources, ICollection<QueryParameter> parameters) {
			var visitor = new SqlGeneratorExpressionVisitor(sources, parameters);
			visitor.Visit(linqExpression);
			return visitor.GetSqlExpression();
		}

		private string GetSqlExpression() {
			return sql.ToString();
		}

		protected override Expression VisitQuerySourceReference(QuerySourceReferenceExpression expression) {
			var source = expression.ReferencedQuerySource;
			string name;
			if (!sources.TryGetValue(source.ItemName, out name))
				throw new InvalidOperationException();

			sql.Append(name);

			return expression;
		}

		protected override Expression VisitSubQuery(SubQueryExpression expression) {
			sql.Append("(");

			// TODO: 

			sql.Append(")");
			return expression;
		}

		protected override Expression VisitBinary(BinaryExpression expression) {
			sql.Append("(");

			Visit(expression.Left);

			switch (expression.NodeType) {
				case ExpressionType.Equal:
					sql.Append(" = ");
					break;
				case ExpressionType.NotEqual:
					sql.Append(" <> ");
					break;
				case ExpressionType.GreaterThan:
					sql.Append(" > ");
					break;
				case ExpressionType.GreaterThanOrEqual:
					sql.Append(" >= ");
					break;
				case ExpressionType.LessThan:
					sql.Append(" < ");
					break;
				case ExpressionType.LessThanOrEqual:
					sql.Append(" <= ");
					break;
				case ExpressionType.Add:
				case ExpressionType.AddChecked:
					sql.Append(" + ");
					break;
				case ExpressionType.Subtract:
				case ExpressionType.SubtractChecked:
					sql.Append(" - ");
					break;
				case ExpressionType.Multiply:
				case ExpressionType.MultiplyChecked:
					sql.Append(" * ");
					break;
				case ExpressionType.Divide:
					sql.Append(" / ");
					break;
				case ExpressionType.Modulo:
					sql.Append(" % ");
					break;
				case ExpressionType.And:
				case ExpressionType.AndAlso:
					sql.Append(" AND ");
					break;
				case ExpressionType.Or:
					sql.Append(" OR ");
					break;
				case ExpressionType.ExclusiveOr:
					sql.Append(" XOR ");
					break;
				default:
					base.VisitBinary(expression);
					break;
			}

			Visit(expression.Right);

			sql.Append(")");

			return expression;
		}

		protected override Expression VisitUnary(UnaryExpression expression) {
			switch (expression.NodeType) {
				case ExpressionType.Not:
					sql.Append("NOT ");
					break;
				case ExpressionType.UnaryPlus:
					sql.Append("+");
					break;
				case ExpressionType.Negate:
					sql.Append("-");
					break;
				default:
					base.VisitUnary(expression);
					break;
			}

			Visit(expression.Operand);

			return expression;
		}

		protected override Expression VisitMember(MemberExpression expression) {
			var type = expression.Member.ReflectedType;
			var mapInfo = Mapper.GetMapInfo(type);
			var memberInfo = mapInfo.Members.FirstOrDefault(x => x.Member.Name == expression.Member.Name);
			if (memberInfo == null)
				throw new InvalidOperationException();

			sql.AppendFormat(".{0}", memberInfo.ColumnName);

			return expression;
		}

		protected override Expression VisitConstant(ConstantExpression expression) {
			return base.VisitConstant(expression);
		}

		protected override Expression VisitMethodCall(MethodCallExpression expression) {
			var methodName = expression.Method.Name;
			var type = expression.Method.DeclaringType;

			if (type == typeof(string)) {
				switch (methodName) {
					case "StartsWith":
						sql.Append("(");
						Visit(expression.Object);
						sql.Append(" LIKE '");
						Visit(expression.Arguments[0]);
						sql.Append("%'");
						sql.Append(")");
						break;
					case "Contains":
						sql.Append("(");
						Visit(expression.Object);
						sql.Append(" LIKE '%");
						Visit(expression.Arguments[0]);
						sql.Append("%'");
						sql.Append(")");
						break;
					case "EndsWith":
						sql.Append("(");
						Visit(expression.Object);
						sql.Append(" LIKE '%");
						Visit(expression.Arguments[0]);
						sql.Append("'");
						sql.Append(")");
						break;
					default:
						throw new NotSupportedException();
				}
			} else if (type == typeof(DateTime) ||
			           type == typeof(DateTimeOffset)) {
				throw new NotImplementedException();
			}

			return expression;
		}
		protected override Exception CreateUnhandledItemException<T>(T unhandledItem, string visitMethod) {
			string itemText = FormatUnhandledItem(unhandledItem);
			var message = string.Format("The expression '{0}' (type: {1}) is not supported by this LINQ provider.", itemText, typeof(T));
			return new NotSupportedException(message);
		}

		private string FormatUnhandledItem<T>(T unhandledItem) {
			var itemAsExpression = unhandledItem as Expression;
			return itemAsExpression != null ? FormattingExpressionVisitor.Format(itemAsExpression) : unhandledItem.ToString();
		}
	}
}
