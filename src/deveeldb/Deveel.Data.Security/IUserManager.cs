// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

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
