using System;
using System.Linq.Expressions;
using System.Reflection;

namespace Deveel.Data.Linq {
	static class TypeUtil {
		public static MemberInfo SelectMember<TSource, TMember>(Expression<Func<TSource, TMember>> selector) {
			Expression body = selector;
			if (body is LambdaExpression) {
				body = ((LambdaExpression)body).Body;
			}

			switch (body.NodeType) {
				case ExpressionType.MemberAccess:
					return ((MemberExpression)body).Member;
				default:
					throw new InvalidOperationException(String.Format("Expression of type '{0}' is not valid for selection of members", body.NodeType));
			}
		}
	}
}
