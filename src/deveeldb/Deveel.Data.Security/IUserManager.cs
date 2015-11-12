using System;

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

		void CreateUserGroup(string groupName);

		bool DropUserGroup(string groupName);

		bool UserGroupExists(string groupName);

		void AddUserToGroup(string userName, string groupName, bool asAdmin);

		bool RemoveUserFromGroup(string userName, string groupName);

		string[] GetUserGroups(string userName);

		bool IsUserInGroup(string userName, string groupName);

		bool IsUserGroupAdmin(string userName, string groupName);
	}
}
