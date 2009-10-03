// 
//  User.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;

namespace Deveel.Data {
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
		/// <summary>
		/// The name of the user.
		/// </summary>
		private readonly String user_name;

		/// <summary>
		/// The database object that this user is currently logged into.
		/// </summary>
		private readonly Database database;

		/// <summary>
		/// The connection string that identifies how this user is connected 
		/// to the database.
		/// </summary>
		private readonly String connection_string;

		/// <summary>
		/// The time this user connected.
		/// </summary>
		private readonly DateTime time_connected;

		/// <summary>
		/// The last time this user executed a command on the connection.
		/// </summary>
		private DateTime last_command_time;

		internal User(String user_name, Database database,
			 String connection_string, DateTime time_connected) {
			this.user_name = user_name;
			this.database = database;
			this.connection_string = connection_string;
			this.time_connected = time_connected;
			last_command_time = time_connected;
		}

		///<summary>
		/// Returns the name of the user.
		///</summary>
		public string UserName {
			get { return user_name; }
		}

		///<summary>
		/// Returns the string that describes how this user is connected
		/// to the engine.
		///</summary>
		/// <remarks>
		/// This is set by the protocol layer.
		/// </remarks>
		public string ConnectionString {
			get { return connection_string; }
		}

		///<summary>
		/// Returns the time the user connected.
		///</summary>
		public DateTime TimeConnected {
			get { return time_connected; }
		}

		///<summary>
		/// Returnst the last time a command was executed by this user.
		///</summary>
		public DateTime LastCommandTime {
			get { return last_command_time; }
		}

		///<summary>
		/// Returns the Database object that this user belongs to.
		///</summary>
		public Database Database {
			get { return database; }
		}

		///<summary>
		/// Refreshes the last time a command was executed by this user.
		///</summary>
		public void RefreshLastCommandTime() {
			last_command_time = DateTime.Now;
		}

		///<summary>
		/// Logs out this user object.
		///</summary>
		/// <remarks>
		/// This will log the user out of the user manager.
		/// </remarks>
		public void Logout() {
			// Clear all triggers for this user,
			UserManager user_manager = database.UserManager;
			if (user_manager != null) {
				user_manager.OnUserLoggedOut(this);
			}
		}

		public bool CanAlterTableObject(DatabaseQueryContext context, TableName table) {
			return database.CanUserAlterTableObject(context, this, table);
		}

		public bool CanCreateTableObject(DatabaseQueryContext context, TableName table) {
			return database.CanUserCreateTableObject(context, this, table);
		}
	}
}