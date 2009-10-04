//  
//  UserManager.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

namespace Deveel.Data {
	/// <summary>
	/// A class that manages the list of users connected to the engine.
	/// </summary>
	/// <remarks>
	/// This class is thread safe, however it is recommended that the callee should
	/// synchronize over this object when inspecting a subset of the user list.
	/// The reason being that a user can connect or disconnect at any time.
	/// </remarks>
	public sealed class UserManager {
		/// <summary>
		/// The list of User objects that are currently connected to the database engine.
		/// </summary>
		private readonly ArrayList user_list;

		internal UserManager() {
			user_list = new ArrayList();
		}

		/// <summary>
		/// Called when a new user connects to the engine.
		/// </summary>
		/// <param name="user"></param>
		internal void OnUserLoggedIn(User user) {
			lock (this) {
				if (!user_list.Contains(user)) {
					user_list.Add(user);
				} else {
					throw new ApplicationException("UserManager already has this User instance logged in.");
				}
			}
		}

		/// <summary>
		/// Called when the user logs out of the engine.
		/// </summary>
		/// <param name="user"></param>
		internal void OnUserLoggedOut(User user) {
			lock (this) {
				user_list.Remove(user);
			}
		}

		/// <summary>
		/// Returns the number of users that are logged in.
		/// </summary>
		public int UserCount {
			get {
				lock (this) {
					return user_list.Count;
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
					return (User) user_list[n];
				}
			}
		}
	}
}