using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Types {
	public static class QueryExtensions {
		public static UserType GetUserType(this IQuery context, ObjectName typeName) {
			return context.GetObject(DbObjectType.Type, typeName) as UserType;
		}

	}
}
