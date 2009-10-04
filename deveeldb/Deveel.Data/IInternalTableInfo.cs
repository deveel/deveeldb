//  
//  IInternalTableInfo.cs
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
		/// Returns the <see cref="DataTableDef"/> object that describes the table 
		/// at the given index in this container.
		/// </summary>
		/// <param name="i"></param>
		/// <returns></returns>
		DataTableDef GetDataTableDef(int i);

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
		IMutableTableDataSource CreateInternalTable(int index);

	}
}