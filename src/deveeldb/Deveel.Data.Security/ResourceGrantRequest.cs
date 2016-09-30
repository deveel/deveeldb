using System;
using System.Runtime.Serialization;

using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	[Serializable]
	public sealed class ResourceGrantRequest : ISerializable {
		public ResourceGrantRequest(ObjectName resource, DbObjectType resourceType, Privileges privileges) { 
			if (resource == null)
				throw new ArgumentNullException("resource");
			if (privileges == Privileges.None)
				throw new ArgumentException("Invalid requested privileges for grant", "privileges");

			Resource = resource;
			ResourceType = resourceType;
			Privileges = privileges;
		}

		private ResourceGrantRequest(SerializationInfo info, StreamingContext context) {
			Resource = (ObjectName) info.GetValue("Resource", typeof(ObjectName));
			ResourceType = (DbObjectType) info.GetInt32("ResourceType");
			Privileges = (Privileges) info.GetInt32("Privileges");
		}

		public ObjectName Resource { get; private set; }

		public DbObjectType ResourceType { get; private set; }

		public Privileges Privileges { get; private set; }

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Resource", Resource);
			info.AddValue("ResourceType", (int) ResourceType);
			info.AddValue("Privileges", (int)Privileges);
		}
	}
}
