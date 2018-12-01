using System;
using System.Threading.Tasks;

using Deveel.Data.Security;

namespace Deveel.Data.Sql.Statements.Security {
	public sealed class PrivilegesRequirement : IRequirement {
		public PrivilegesRequirement(DbObjectType objectType, ObjectName objectName, Privilege privilege) {
			ObjectType = objectType;
			ObjectName = objectName ?? throw new ArgumentNullException(nameof(objectName));
			Privilege = privilege;
		}

		public Privilege Privilege { get; }

		public DbObjectType ObjectType { get; }

		public ObjectName ObjectName { get; }

		async Task IRequirement.HandleRequirementAsync(IContext context) {
			if (!await context.UserHasPrivileges(ObjectType, ObjectName, Privilege))
				throw new UnauthorizedAccessException($"The user '{context.User().Name}' misses the privilege {Privilege} on {ObjectName}");
		}
	}
}