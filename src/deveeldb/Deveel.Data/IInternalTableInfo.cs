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
	/// A class that acts as a container for any system tables that are 
	/// generated from information inside the database engine.
	/// </summary>
	/// <remarks>
	/// For example, the database statistics table is an internal system 
	/// table, as well as the table that describes all database table 
	/// information, etc.
	/// <para>
	/// This object acts as a container and factory for generating such tables.
	/// </para>
	/// <para>
	/// Note that implementations of this object should be thread-safe and
	/// immutable so we can create static global implementations.
	/// </para>
	/// </remarks>
	interface IInternalTableInfo {
		/// <summary>
		/// Returns the number of internal table sources that this 
		/// object is maintaining.
		/// </summary>
		int TableCount { get; }

		/// <summary>
		/// Finds the index in this container of the given table name, 
		/// otherwise returns -1.
		/// </summary>
		/// <param name="name"></param>
		/// <returns></returns>
		int FindTableName(TableName name);

		/// <summary>
		/// Returns the name of the table at the given index in this container.
		/// </summary>
		/// <param name="i"></param>
		/// <returns></returns>
		TableName GetTableName(int i);

		/// <summary>
		/// Returns the <see cref="DataTableInfo"/> object that describes the table 
		/// at the given index in this container.
		/// </summary>
		/// <param name="i"></param>
		/// <returns></returns>
		DataTableInfo GetTableInfo(int i);

		/// <summary>
		/// Returns true if this container contains a table with the given name.
		/// </summary>
		/// <param name="name"></param>
		/// <returns></returns>
		bool ContainsTableName(TableName name);

		/// <summary>
		/// Returns a String that describes the type of the table at the given index.
		/// </summary>
		/// <param name="i"></param>
		/// <returns></returns>
		String GetTableType(int i);

		/// <summary>
		/// This is the factory method for generating the internal table for the 
		/// given table in this container.
		/// </summary>
		/// <param name="index"></param>
		/// <remarks>
		/// This should return an implementation of <see cref="IMutableTableDataSource"/> that 
		/// is used to represent the internal data being modelled.
		/// <para>
		/// This method is allowed to throw an exception for table objects that aren't backed by 
		/// a <see cref="IMutableTableDataSource"/>, such as a view.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		ITableDataSource CreateInternalTable(int index);

	}
}