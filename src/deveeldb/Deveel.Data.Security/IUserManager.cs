using System;
using System.Collections.Generic;

using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	public interface IUserManager : IDisposable {
		void CreateUser(UserInfo userInfo, string identifier);

		bool DropUser(string userName);

		void AlterUser(UserInfo userInfo, string identifier);

		void SetUserStatus(string userName, UserStatus status);

		UserStatus GetUserStatus(string userName);

		bool CheckIdentifier(string userName, string identifier);

		bool UserExists(string userName);

		UserInfo GetUser(string userName);

		void GrantToUser(string userName, UserGrant grant);

		Privileges GetUserPrivileges(string userName, DbObjectType objectType, ObjectName objectName, bool withOption);

		void RevokeFromUser(string userName, UserGrant grant);

		void CreateUserGroup(string groupName);

		bool DropUserGroup(string groupName);

		void GrantToGroup(string groupName, Privileges privileges);

		void RevokeFromGroup(string groupName, Privileges privileges);

		void AddUserToGroup(string userName, string groupName);

		string[] GetUserGroups(string userName);

		bool IsUserInGroup(string userName, string groupName);

	}
}
