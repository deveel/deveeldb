using System;

using Deveel.Data.Security;

namespace Deveel.Data.Sql.Schemas {
	public static class QueryExtensions {
		public static void CreateSchema(this IQuery context, string name, string type) {
			if (!context.UserCanCreateSchema())
				throw new InvalidOperationException();      // TODO: throw a specialized exception

			context.CreateObject(new SchemaInfo(name, type));
		}

		public static bool SchemaExists(this IQuery context, string name) {
			return context.ObjectExists(DbObjectType.Schema, new ObjectName(name));
		}

		public static ObjectName ResolveSchemaName(this IQuery context, string name) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			return context.ResolveObjectName(DbObjectType.Schema, new ObjectName(name));
		}
	}
}
