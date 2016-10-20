using System;

using Deveel.Data.Routines;
using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	public sealed class ResourceAccessSecurityAssert : ISecurityAssert {
		public ResourceAccessSecurityAssert(ObjectName resourceName, DbObjectType resourceType, Privileges privileges) {
			ResourceName = resourceName;
			ResourceType = resourceType;
			Privileges = privileges;
			Arguments = new InvokeArgument[0];
		}

		public ResourceAccessSecurityAssert(ObjectName routineName, InvokeArgument[] arguments)
			: this(routineName, DbObjectType.Routine, Privileges.Execute) {
			Arguments = arguments;
		}

		public ObjectName ResourceName { get; private set; }

		public DbObjectType ResourceType { get; private set; }

		public Privileges Privileges { get; private set; }

		public InvokeArgument[] Arguments { get; private set; }

		AssertResult ISecurityAssert.Assert(ISecurityContext context) {
			if ((Privileges & Privileges.Create) != 0) {
				if (ResourceType == DbObjectType.Schema) {
					if (!context.User.CanManageSchema())
						return AssertResult.Deny(new MissingPrivilegesException(context.User.Name, ResourceName, Privileges));
				}
			}

			if ((Privileges & Privileges.Execute) != 0) {
				var invoke = new Invoke(ResourceName, Arguments);
				if (context.Request.Access().IsSystemFunction(invoke, context.Request))
					return AssertResult.Allow();
			}

			if (!context.User.HasPrivileges(ResourceType, ResourceName, Privileges))
				return AssertResult.Deny(new MissingPrivilegesException(context.User.Name, ResourceName, Privileges));

			return AssertResult.Allow();
		}
	}
}
