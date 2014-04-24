// 
//  Copyright 2010-2014  Deveel
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

using System;
using System.Collections.Generic;

namespace Deveel.Data.Security {
	/// <summary>
	/// A class that manages the list of users connected to the engine.
	/// </summary>
	/// <remarks>
	/// This class is thread safe, however it is recommended that the callee should
	/// synchronize over this object when inspecting a subset of the user list.
	/// The reason being that a user can connect or disconnect at any time.
	/// </remarks>
	public sealed class LoggedUsers {
		/// <summary>
		/// The list of User objects that are currently connected to the database engine.
		/// </summary>
		private readonly List<User> userList;

		internal LoggedUsers() {
			userList = new List<User>();
		}

		/// <summary>
		/// Called when a new user connects to the engine.
		/// </summary>
		/// <param name="user"></param>
		internal void OnUserLoggedIn(User user) {
			lock (this) {
				if (userList.Contains(user))
					throw new ApplicationException("LoggedUsers already has this User instance logged in.");

				userList.Add(user);
			}
		}

		/// <summary>
		/// Called when the user logs out of the engine.
		/// </summary>
		/// <param name="user"></param>
		internal void OnUserLoggedOut(User user) {
			lock (this) {
				userList.Remove(user);
			}
		}

		/// <summary>
		/// Returns the number of users that are logged in.
		/// </summary>
		public int UserCount {
			get {
				lock (this) {
					return userList.Count;
				}
			}
		}

		/// <summary>
		/// Returns the User object at index 'n' in the manager where 0 is the first user.
		/// </summary>
		/// <param name="n"></param>
		/// <returns></returns>
		public User this[int n] {
			get {
				lock (this) {
					return userList[n];
				}
			}
		}
	}
}