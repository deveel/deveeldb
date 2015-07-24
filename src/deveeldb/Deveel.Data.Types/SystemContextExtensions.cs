using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Types;

namespace Deveel.Data.Types {
	public static class SystemContextExtensions {
		public static DataType ResolveType(this ISystemContext context, string typeName, params DataTypeMeta[] meta) {
			var resolvers = context.ServiceProvider.ResolveAll<ITypeResolver>();
			var resolveContext = new TypeResolveContext(SqlTypeCode.Type, typeName, meta);
			foreach (var typeResolver in resolvers) {
				var type = typeResolver.ResolveType(resolveContext);
				if (type != null)
					return type;
			}

			return null;
		}
	}
}
