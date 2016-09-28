using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	public sealed class ResourceAccessRequest {
		public ResourceAccessRequest(ObjectName resource, DbObjectType resourceType, Privileges privileges) {
			if (resource == null)
				throw new ArgumentNullException("resource");

			Resource = resource;
			ResourceType = resourceType;
			Privileges = privileges;
		}

		public ObjectName Resource { get; private set; }

		public DbObjectType ResourceType { get; private set; }

		public Privileges Privileges { get; private set; }
	}
}
