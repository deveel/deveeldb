using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Types;

namespace Deveel.Data.Types {
	public static class SystemContextExtensions {
		public static DataType ResolveType(this ISystemContext context, string typeName, params DataTypeMeta[] meta) {
			var resolvers = context.ServiceProvider.ResolveAll<ITypeResolver>();
			foreach (var typeResolver in resolvers) {
				var type = typeResolver.ResolveType(typeName, meta);
				if (type != null)
					return type;
			}

			return null;
		}
	}
}
