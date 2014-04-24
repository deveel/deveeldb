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

using Deveel.Data.DbSystem;

namespace Deveel.Data.Security {
	/// <summary>
	/// Encapsulates the information about a single user logged into the system.
	/// </summary>
	/// <remarks>
	/// The class provides access to information in the user database.
	/// <para>
	/// This object also serves as a storage for session state information. For
	/// example, this object stores the triggers that this session has created.
	/// </para>
	/// <para>
	/// <b>Note</b> This object is not immutable. The same user may log into 
	/// the system and it will result in a new User object being created.
	/// </para>
	/// </remarks>
	public sealed class User {
		internal User(string userName, Database database, string connectionString, DateTime timeConnected) {
			UserName = userName;
			Database = database;
			ConnectionString = connectionString;
			TimeConnected = timeConnected;
			LastCommandTime = timeConnected;
		}

		///<summary>
		/// Returns the name of the user.
		///</summary>
		public string UserName { get; private set; }

		///<summary>
		/// Returns the string that describes how this user is connected
		/// to the engine.
		///</summary>
		/// <remarks>
		/// This is set by the protocol layer.
		/// </remarks>
		public string ConnectionString { get; private set; }

		///<summary>
		/// Returns the time the user connected.
		///</summary>
		public DateTime TimeConnected { get; private set; }

		///<summary>
		/// Returnst the last time a command was executed by this user.
		///</summary>
		public DateTime LastCommandTime { get; private set; }

		///<summary>
		/// Returns the Database object that this user belongs to.
		///</summary>
		public Database Database { get; private set; }

		///<summary>
		/// Refreshes the last time a command was executed by this user.
		///</summary>
		internal void RefreshLastCommandTime() {
			LastCommandTime = DateTime.Now;
		}

		///<summary>
		/// Logs out this user object.
		///</summary>
		/// <remarks>
		/// This will log the user out of the user manager.
		/// </remarks>
		internal void Logout() {
			// Clear all triggers for this user,
			LoggedUsers loggedUsers = Database.LoggedUsers;
			if (loggedUsers != null) {
				loggedUsers.OnUserLoggedOut(this);
			}
		}

		/// <summary>
		/// The username of the internal secure user.
		/// </summary>
		/// <remarks>
		/// The internal secure user is only used for internal highly privileged 
		/// operations. This user is given full privs to everything and is used to 
		/// manage the system tables, for authentication, etc.
		/// </remarks>
		public const String SystemName = "@SYSTEM";

		/// <summary>
		/// The string representing the public user (privs granted to all users).
		/// </summary>
		public const String PublicName = "@PUBLIC";
	}
}