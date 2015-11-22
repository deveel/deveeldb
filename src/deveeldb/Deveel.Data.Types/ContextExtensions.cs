using System;

using Deveel.Data.Services;

namespace Deveel.Data.Types {
	public static class ContextExtensions {
		public static SqlType ResolveType(this IContext context, string typeName, params DataTypeMeta[] meta) {
			if (PrimitiveTypes.IsPrimitive(typeName))
				return PrimitiveTypes.Resolve(typeName, meta);

			var resolvers = context.ResolveAllServices<ITypeResolver>();
			foreach (var resolver in resolvers) {
				var sqlType = resolver.ResolveType(new TypeResolveContext(SqlTypeCode.Unknown, typeName, meta));
				if (sqlType != null)
					return sqlType;
			}

			return null;
		}

		public static ITypeResolver TypeResolver(this IContext context) {
			return new ContextTypeResolver(context);
		}

		#region ContextTypeResolver

		class ContextTypeResolver : ITypeResolver {
			private readonly IContext queryContext;

			public ContextTypeResolver(IContext context) {
				queryContext = context;
			}

			public SqlType ResolveType(TypeResolveContext context) {
				return queryContext.ResolveType(context.TypeName, context.GetMeta());
			}
		}

		#endregion
	}
}
