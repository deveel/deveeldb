using System;

using Deveel.Data.Services;

namespace Deveel.Data.Types {
	public static class ContextExtensions {
		public static SqlType ResolveType(this IContext context, string typeName, params DataTypeMeta[] meta) {
			throw new NotImplementedException();
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
