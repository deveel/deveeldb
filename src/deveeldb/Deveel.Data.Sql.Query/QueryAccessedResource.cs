using System;

namespace Deveel.Data.Sql.Query {
	public sealed class QueryAccessedResource {
		public QueryAccessedResource(ObjectName resourceName, DbObjectType resourceType) {
			if (resourceName == null)
				throw new ArgumentNullException("resourceName");

			ResourceName = resourceName;
			ResourceType = resourceType;
		}

		public ObjectName ResourceName { get; private set; }

		public DbObjectType ResourceType { get; private set; }
	}
}
