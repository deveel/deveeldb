using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	public sealed class ResourceGrantAction : ISecurityPostExecuteAction {
		public ResourceGrantAction(ObjectName resourceName, DbObjectType resourceType, Privileges privileges) {
			ResourceName = resourceName;
			ResourceType = resourceType;
			Privileges = privileges;
		}

		public ObjectName ResourceName { get; private set; }

		public DbObjectType ResourceType { get; private set; }

		public Privileges Privileges { get; private set; }

		void ISecurityPostExecuteAction.OnActionExecuted(ISecurityContext context) {
			try {
				context.Request.Access().GrantOn(ResourceType, ResourceName, context.User.Name, Privileges, true);
			} catch (SecurityException) {
				throw;
			} catch (Exception ex) {
				throw new SecurityException(
					String.Format("An error occurred while granting '{0}' to '{1}' on '{2}'.", Privileges, ResourceName,
						context.User.Name), ex);
			}
		}
	}
}
