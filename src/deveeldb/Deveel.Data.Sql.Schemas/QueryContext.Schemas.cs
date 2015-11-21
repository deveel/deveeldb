using System;

using Deveel.Data.Security;

namespace Deveel.Data.Sql.Schemas {
	public static class QueryContext {
		public static void CreateSchema(this IQueryContext context, string name, string type) {
			if (!context.UserCanCreateSchema())
				throw new InvalidOperationException();      // TODO: throw a specialized exception

			context.CreateObject(new SchemaInfo(name, type));
		}

		public static bool SchemaExists(this IQueryContext context, string name) {
			return context.ObjectExists(DbObjectType.Schema, new ObjectName(name));
		}

		public static ObjectName ResolveSchemaName(this IQueryContext context, string name) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			return context.ResolveObjectName(DbObjectType.Schema, new ObjectName(name));
		}
	}
}
