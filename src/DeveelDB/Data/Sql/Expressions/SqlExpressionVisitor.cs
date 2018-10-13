// 
//  Copyright 2010-2018 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Expressions {
	public class SqlExpressionVisitor {
		public virtual SqlExpression Visit(SqlExpression expression) {
			if (expression == null)
				return null;

			switch (expression.ExpressionType) {
				case SqlExpressionType.Add:
				case SqlExpressionType.Subtract:
				case SqlExpressionType.Divide:
				case SqlExpressionType.Multiply:
				case SqlExpressionType.Modulo:
				case SqlExpressionType.Is:
				case SqlExpressionType.IsNot:
				case SqlExpressionType.Equal:
				case SqlExpressionType.NotEqual:
				case SqlExpressionType.GreaterThan:
				case SqlExpressionType.GreaterThanOrEqual:
				case SqlExpressionType.LessThan:
				case SqlExpressionType.LessThanOrEqual:
				case SqlExpressionType.And:
				case SqlExpressionType.Or:
				case SqlExpressionType.XOr:
					return VisitBinary((SqlBinaryExpression) expression);
				case SqlExpressionType.Like:
				case SqlExpressionType.NotLike:
					return VisitStringMatch((SqlStringMatchExpression) expression);
				case SqlExpressionType.Not:
				case SqlExpressionType.Negate:
				case SqlExpressionType.UnaryPlus:
					return VisitUnary((SqlUnaryExpression) expression);
				//case SqlExpressionType.Any:
				//case SqlExpressionType.All:
				//	return VisitQuantify((SqlQuantifyExpression) expression);
				case SqlExpressionType.Cast:
					return VisitCast((SqlCastExpression) expression);
				case SqlExpressionType.Reference:
					return VisitReference((SqlReferenceExpression)expression);
				case SqlExpressionType.Variable:
					return VisitVariable((SqlVariableExpression)expression);
				case SqlExpressionType.ReferenceAssign:
					return VisitReferenceAssign((SqlReferenceAssignExpression)expression);
				case SqlExpressionType.VariableAssign:
					return VisitVariableAssign((SqlVariableAssignExpression)expression);
				//case SqlExpressionType.Function:
				//	return VisitFunction((SqlFunctionExpression) expression);
				case SqlExpressionType.Condition:
					return VisitCondition((SqlConditionExpression) expression);
				case SqlExpressionType.Parameter:
					return VisitParameter((SqlParameterExpression) expression);
				case SqlExpressionType.Constant:
					return VisitConstant((SqlConstantExpression) expression);
				//case SqlExpressionType.Query:
				//	return VisitQuery((SqlQueryExpression) expression);
				case SqlExpressionType.Group:
					return VisitGroup((SqlGroupExpression) expression);
				default:
					throw new SqlExpressionException($"Invalid expression type: {expression.ExpressionType}");
			}
		}

		public virtual SqlExpression VisitStringMatch(SqlStringMatchExpression expression) {
			var left = expression.Left;
			var pattern = expression.Pattern;
			var escape = expression.Escape;

			if (left != null)
				left = Visit(left);
			if (pattern != null)
				pattern = Visit(pattern);
			if (escape != null)
				escape = Visit(escape);

			return SqlExpression.StringMatch(expression.ExpressionType, left, pattern, escape);
		}

		//public virtual InvokeArgument[] VisitInvokeArguments(IList<InvokeArgument> arguments) {
		//	if (arguments == null)
		//		return null;

		//	var result = new InvokeArgument[arguments.Count];
		//	for (int i = 0; i < arguments.Count; i++) {
		//		result[i] = VisitInvokeArgument(arguments[i]);
		//	}

		//	return result;
		//}

		//public virtual InvokeArgument VisitInvokeArgument(InvokeArgument argument) {
		//	var value = argument.Value;
		//	if (value != null)
		//		value = Visit(value);

		//	return new InvokeArgument(argument.Name, value);
		//}

		//public virtual SqlExpression VisitFunction(SqlFunctionExpression expression) {
		//	var args = VisitInvokeArguments(expression.Arguments);
		//	return SqlExpression.Function(expression.FunctionName, args);
		//}

		public virtual SqlExpression VisitGroup(SqlGroupExpression expression) {
			var child = expression.Expression;
			if (child != null)
				child = Visit(child);

			return SqlExpression.Group(child);
		}

		//public virtual SqlExpression VisitQuantify(SqlQuantifyExpression expression) {
		//	var exp = expression.Expression;
		//	if (exp != null)
		//		exp = (SqlBinaryExpression) Visit(exp);

		//	return SqlExpression.Quantify(expression.ExpressionType, exp);
		//}

		public virtual SqlExpression VisitCondition(SqlConditionExpression expression) {
			var test = expression.Test;
			if (test != null)
				test = Visit(test);

			var ifTrue = expression.IfTrue;
			if (ifTrue != null)
				ifTrue = Visit(ifTrue);

			var ifFalse = expression.IfFalse;
			if (ifFalse != null)
				ifFalse = Visit(ifFalse);

			return SqlExpression.Condition(test, ifTrue, ifFalse);
		}

		public virtual SqlExpression VisitVariableAssign(SqlVariableAssignExpression expression) {
			var value = expression.Value;
			if (value != null)
				value = Visit(value);

			return SqlExpression.VariableAssign(expression.VariableName, value);
		}

		public virtual SqlExpression VisitReference(SqlReferenceExpression expression) {
			return expression;
		}

		public virtual SqlExpression VisitVariable(SqlVariableExpression expression) {
			return expression;
		}

		public virtual SqlExpression VisitReferenceAssign(SqlReferenceAssignExpression expression) {
			var value = expression.Value;
			if (value != null)
				value = Visit(value);

			return SqlExpression.ReferenceAssign(expression.ReferenceName, value);
		}

		public virtual SqlExpression VisitCast(SqlCastExpression expression) {
			var value = expression.Value;
			if (value != null)
				value = Visit(value);

			return SqlExpression.Cast(value, expression.TargetType);
		}

		public virtual SqlExpression VisitBinary(SqlBinaryExpression expression) {
			var left = expression.Left;
			var right = expression.Right;
			if (left != null)
				left = Visit(left);
			if (right != null)
				right = Visit(right);

			return SqlExpression.Binary(expression.ExpressionType, left, right);
		}

		public virtual SqlExpression VisitUnary(SqlUnaryExpression expression) {
			var operand = expression.Operand;
			if (operand != null)
				operand = Visit(operand);

			return SqlExpression.Unary(expression.ExpressionType, operand);
		}

		public virtual SqlExpression VisitConstant(SqlConstantExpression expression) {
			return SqlExpression.Constant(expression.Value);
		}

		public virtual SqlExpression VisitParameter(SqlParameterExpression expression) {
			return expression;
		}

		//public virtual SqlExpression VisitQuery(SqlQueryExpression expression) {
		//	var items = VisitQueryItems(expression.Items);

		//	var query = new SqlQueryExpression {
		//		Distinct = expression.Distinct
		//	};

		//	if (items != null) {
		//		foreach (var item in items) {
		//			query.Items.Add(item);
		//		}
		//	}

		//	var from = expression.From;
		//	if (from != null)
		//		from = VisitQueryFrom(from);

		//	query.From = from;

		//	var where = expression.Where;
		//	if (where != null)
		//		where = Visit(where);

		//	query.Where = where;

		//	var having = expression.Having;
		//	if (having != null)
		//		having = Visit(having);

		//	query.Having = having;

		//	query.GroupBy = VisitExpressionList(expression.GroupBy);
		//	query.GroupMax = expression.GroupMax;

		//	return query;
		//}

		public virtual IList<SqlExpression> VisitExpressionList(IList<SqlExpression> list) {
			if (list == null)
				return null;

			var result = new List<SqlExpression>();

			for (int i = 0; i < list.Count; i++) {
				result.Add(Visit(list[i]));
			}

			return result;
		}

		//public virtual IList<SqlQueryExpressionItem> VisitQueryItems(IList<SqlQueryExpressionItem> items) {
		//	List<SqlQueryExpressionItem> result = null;
		//	if (items != null) {
		//		result = new List<SqlQueryExpressionItem>(items.Count);
		//		foreach (var item in items) {
		//			result.Add(VisitQueryItem(item));
		//		}
		//	}

		//	return result;
		//}

		//public virtual SqlQueryExpressionItem VisitQueryItem(SqlQueryExpressionItem item) {
		//	var expression = item.Expression;
		//	if (expression != null)
		//		expression = Visit(expression);

		//	return new SqlQueryExpressionItem(expression, item.Alias);
		//}

		//public virtual SqlQueryExpressionFrom VisitQueryFrom(SqlQueryExpressionFrom from) {
		//	var result = new SqlQueryExpressionFrom();

		//	var sources = from.Sources.ToList();
		//	for (int i = 0; i < sources.Count; i++) {
		//		result.Source(VisitQuerySource(sources[i]));

		//		if (i > 0 && !from.IsNaturalJoin) {
		//			var part = VisitJoinPart(from.GetJoinPart(i - 1));
		//			result.Join(part.JoinType, part.OnExpression);
		//		}
		//	}

		//	return result;
		//}

		//public virtual SqlQueryExpressionSource VisitQuerySource(SqlQueryExpressionSource source) {
		//	if (source.IsTable)
		//		return new SqlQueryExpressionSource(source.TableName, source.Alias);

		//	var query = source.Query;
		//	if (query != null)
		//		query = (SqlQueryExpression) Visit(query);

		//	return new SqlQueryExpressionSource(query, source.Alias);
		//}

		//public virtual JoinPart VisitJoinPart(JoinPart part) {
		//	var onExpression = part.OnExpression;
		//	if (onExpression != null)
		//		onExpression = Visit(onExpression);

		//	if (part.IsQuery)
		//		return new JoinPart(part.JoinType, part.Query, onExpression);

		//	return new JoinPart(part.JoinType, part.TableName, onExpression);
		//}
	}
}