// 
//  Copyright 2010  Deveel
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

using Deveel.Data.Control;

namespace Deveel.Data.Protocol {
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
		IDatabaseInterface Create(String username, String password, DbConfig config);

		///<summary>
        /// Boots the database with the given configuration.
		///</summary>
        ///<param name="config">The configuration variables.</param>
		///<returns>
        /// Returns a <see cref="IDatabaseInterface"/> for talking to the database.
		/// </returns>
		IDatabaseInterface Boot(DbConfig config);

		///<summary>
        /// Attempts to test if the database exists or not.
		///</summary>
		///<returns>
		/// Returns true if the database exists, otherwise false.
		/// </returns>
		bool CheckExists();

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