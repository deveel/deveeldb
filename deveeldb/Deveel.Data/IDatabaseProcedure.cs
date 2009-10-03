// 
//  IDatabaseProcedure.cs
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
	/// This interface represents a database procedure that is executed on 
	/// the server side.
	/// </summary>
	/// <remarks>
	/// It is used to perform database specific functions that can only be 
	/// performed on the server.
	/// <para>
	/// A procedure must manage its own table locking.
	/// </para>
	/// </remarks>
	public interface IDatabaseProcedure {
		///<summary>
		/// Executes the procudure and returns the resultant table.
		///</summary>
		///<param name="user"></param>
		///<param name="args"></param>
		/// <remarks>
		/// Note, the args have to be serializable.  There may be only 0 to 
		/// 16 arguments. The method may throw a 'DatabaseException' if the procedure 
		/// failed.
		/// </remarks>
		///<returns></returns>
		Table Execute(User user, Object[] args);

		///<summary>
		/// This returns a DataTable[] array that lists the DataTables that are Read
		/// during this procedure.
		///</summary>
		///<param name="db"></param>
		///<returns></returns>
		DataTable[] GetReadTables(DatabaseConnection db);

		///<summary>
		/// Returns a DataTable[] array that lists the DataTables that are written
		/// to during this procedure.
		///</summary>
		///<param name="db"></param>
		///<returns></returns>
		DataTable[] GetWriteTables(DatabaseConnection db);

		///<summary>
		/// Returns the locking mode in which the database operates.
		///</summary>
		LockingMode LockingMode { get; }

		///<summary>
		/// Sets the LockHandle object for this procedure.
		///</summary>
		///<param name="lock_handle"></param>
		/// <remarks>
		/// This should be called after the tables that this procedure 
		/// uses have been locked.
		/// </remarks>
		void SetLockHandle(LockHandle lock_handle);
	}
}