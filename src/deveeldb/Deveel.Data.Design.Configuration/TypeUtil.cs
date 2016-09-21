using System;
using System.Linq.Expressions;
using System.Reflection;

namespace Deveel.Data.Design.Configuration {
	static class TypeUtil {
		public static MemberInfo FindMember<TEntity, TMember>(Expression<Func<TEntity, TMember>> selector) {
			var type = typeof(TEntity);

			MemberExpression member = selector.Body as MemberExpression;
			if (member == null)
				throw new ArgumentException(string.Format(
					"Expression '{0}' refers to a method, not a property.",
					selector.ToString()));

			var memberInfo = member.Member;

			if (type != memberInfo.ReflectedType &&
				!type.IsSubclassOf(memberInfo.ReflectedType))
				throw new ArgumentException(string.Format(
					"Expression '{0}' refers to a property that is not from type {1}.", selector, type));

			return memberInfo;
		}
	}
}
