using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Types {
	public static class QueryContextExtensions {
		public static DataType ResolveType(this IQueryContext context, string typeName, params DataTypeMeta[] meta) {
			var type = context.SystemContext().ResolveType(typeName, meta);
			if (type != null)
				return type;

			var fullTypeName = context.ResolveObjectName(typeName);
			return context.GetUserType(fullTypeName);
		}
	}
}
