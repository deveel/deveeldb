// 
//  IProcedureConnection.cs
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