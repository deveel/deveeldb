using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Types {
	public static class QueryContext {
		public static UserType GetUserType(this IQueryContext context, ObjectName typeName) {
			return context.GetObject(DbObjectType.Type, typeName) as UserType;
		}

		public static SqlType ResolveType(this IQueryContext context, string typeName, params DataTypeMeta[] meta) {
			var type = context.SystemContext().ResolveType(typeName, meta);
			if (type != null)
				return type;

			var fullTypeName = context.ResolveObjectName(typeName);
			return context.GetUserType(fullTypeName);
		}

		public static ITypeResolver TypeResolver(this IQueryContext context) {
			return new ContextTypeResolver(context);
		}

		#region ContextTypeResolver

		class ContextTypeResolver : ITypeResolver {
			private readonly IQueryContext queryContext;

			public ContextTypeResolver(IQueryContext context) {
				queryContext = context;
			}

			public SqlType ResolveType(TypeResolveContext context) {
				return queryContext.ResolveType(context.TypeName, context.GetMeta());
			}
		}

		#endregion
	}
}
