// 
//  ILocalBootable.cs
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

using Deveel.Data.Control;

namespace Deveel.Data.Client {
	///<summary>
    /// An interface that is implemented by an object that boots up 
    /// the database.
	///</summary>
	/// <remarks>
	/// This is provided as an interface so that we aren't dependant 
	/// on the entire database when compiling the client code.
	/// </remarks>
	public interface ILocalBootable {
		///<summary>
		/// Attempts to create a new database system with the given name, and the
		/// given username/password as the admin user for the system.
		///</summary>
		///<param name="username"></param>
		///<param name="password"></param>
        ///<param name="config">The configuration variables.</param>
		/// <remarks>
		/// Once created, the newly created database will be booted up.
		/// </remarks>
		///<returns>
        /// Returns a <see cref="IDatabaseInterface"/> for talking to the database.
		/// </returns>
		IDatabaseInterface Create(String username, String password, IDbConfig config);

		///<summary>
        /// Boots the database with the given configuration.
		///</summary>
        ///<param name="config">The configuration variables.</param>
		///<returns>
        /// Returns a <see cref="IDatabaseInterface"/> for talking to the database.
		/// </returns>
		IDatabaseInterface Boot(IDbConfig config);

		///<summary>
        /// Attempts to test if the database exists or not.
		///</summary>
        ///<param name="config">The configuration variables.</param>
		///<returns>
		/// Returns true if the database exists, otherwise false.
		/// </returns>
		bool CheckExists(IDbConfig config);

	    ///<summary>
	    /// Returns true if there is a database currently booted in the current 
	    /// runtime, otherwise returns false.
	    ///</summary>
	    bool IsBooted { get; }

		///<summary>
        /// Connects this interface to the database currently running in this runtime.
		///</summary>
		///<returns>
        /// Returns a <see cref="IDatabaseInterface"/> for talking to the database.
		/// </returns>
		IDatabaseInterface Connect();

	}
}