using System;

namespace Deveel.Data.Sql.Query {
	public sealed class QueryAccessedResource {
		public QueryAccessedResource(ObjectName resourceName, DbObjectType resourceType) {
			ResourceName = resourceName;
			ResourceType = resourceType;
		}

		public ObjectName ResourceName { get; private set; }

		public DbObjectType ResourceType { get; private set; }
	}
}
