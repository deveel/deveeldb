using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	public interface IPrivilegeManager : IDisposable {
		void GrantToUser(string userName, Grant grant);

		Privileges GetUserPrivileges(string userName, DbObjectType objectType, ObjectName objectName, bool withOption);

		void RevokeFromUser(string userName, Grant grant);

		void GrantToGroup(string groupName, Grant grant);

		void RevokeFromGroup(string groupName, Grant grant);

		Privileges GetGroupPrivileges(string groupName, DbObjectType objectType, ObjectName objectName);
	}
}
