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