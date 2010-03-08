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
using System.Data;

namespace Deveel.Data {
	///<summary>
	/// An interface for accessing a database connection inside a stored procedure.
	///</summary>
	public interface IProcedureConnection {
		///<summary>
		/// Returns an ADO.NET IDbConnection implementation for executing queries 
		/// on this connection.
		///</summary>
		/// <remarks>
		/// The <see cref="IDbConnection">connection</see> has auto-commit turned off, 
		/// and it disables the ability for the connection to <i>commit</i> changes to 
		/// the database.
		/// <para>
		/// This method is intended to provide the procedure developer with a convenient 
		/// and consistent way to query and manipulate the database from the body of a 
		/// stored procedure method.
		/// </para>
		/// <para>
		/// The <see cref="IDbConnection"/> object returned here may invalidate when the 
		/// procedure invokation call ends so the returned object must not be cached to be 
		/// used again later.
		/// </para>
		/// <para>
		/// The returned <see cref="IDbConnection"/> object is NOT thread safe and should 
		/// only be used by a single thread.  Accessing this connection from multiple threads 
		/// will result in undefined behaviour.
		/// </para>
		/// <para>
		/// The <see cref="IDbConnection">connection</see> object returned here has the same 
		/// privs as the user who owns the stored procedure.
		/// </para>
		/// </remarks>
		///<returns></returns>
		IDbConnection GetDbConnection();

		///<summary>
		/// Returns the Database object for this database providing access to various
		/// general database features including backing up replication and configuration.
		///</summary>
		/// <remarks>
		/// Some procedures may not be allowed access to this object in which case a 
		/// ProcedureException is thrown notifying of the security violation.
		/// </remarks>
		Database Database { get; }
	}
}