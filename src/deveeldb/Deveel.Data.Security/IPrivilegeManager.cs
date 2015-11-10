using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	public interface IPrivilegeManager : IDisposable {
		void GrantToUser(string userName, UserGrant grant);

		Privileges GetUserPrivileges(string userName, DbObjectType objectType, ObjectName objectName, bool withOption);

		void RevokeFromUser(string userName, UserGrant grant);

		void GrantToGroup(string groupName, Privileges privileges);

		void RevokeFromGroup(string groupName, Privileges privileges);

		Privileges GetGroupPrivileges(string groupName);
	}
}
