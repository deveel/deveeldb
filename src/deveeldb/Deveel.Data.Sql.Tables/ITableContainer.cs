// 
//  Copyright 2010-2015 Deveel
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
//

using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Sql.Tables {
	/// <summary>
	/// A container for any system tables that are generated from information 
	/// inside the database engine.
	/// </summary>
	/// <remarks>
	/// Implementations of this contract expose system tables that have
	/// a read-only access, and used to materialize system information
	/// as <see cref="ITable"/>
	/// </remarks>
	public interface ITableContainer {
		/// <summary>
		/// Returns the total count of tables that this object is maintaining.
		/// </summary>
		int TableCount { get; }

		/// <summary>
		/// Finds the index in this container of the given table by its name.
		/// </summary>
		/// <param name="name">The name of the table to find.</param>
		/// <returns>
		/// Returns a zero-based index that represents the offset of the table
		/// identified by the given name within the context, ot <c>-1</c> of not found.
		/// </returns>
		int FindByName(ObjectName name);

		/// <summary>
		/// Gets the name of the table at the given index in this container.
		/// </summary>
		/// <param name="offset">The zero-based offset of the table whose name to return.</param>
		/// <returns>
		/// Returns a <seealso cref="ObjectName"/> that identifies the table.
		/// </returns>
		/// <exception cref="ArgumentOutOfRangeException">
		/// If the given <paramref name="offset"/> is smaller than 0 or greater or equal than
		/// <see cref="TableCount"/>.
		/// </exception>
		ObjectName GetTableName(int offset);

		/// <summary>
		/// Gets the information of the table at the given offset in this container.
		/// </summary>
		/// <param name="offset">The zero-based offset of the table whose information to return.</param>
		/// <returns>
		/// Returns an instance of <seealso cref="TableInfo"/> that describes the metadata of a table
		/// at the given offset within the context.
		/// </returns>
		/// <exception cref="ArgumentOutOfRangeException">
		/// If the given <paramref name="offset"/> is smaller than 0 or greater or equal than
		/// <see cref="TableCount"/>.
		/// </exception>
		TableInfo GetTableInfo(int offset);

		/// <summary>
		/// Gets the type of the table at the given offset.
		/// </summary>
		/// <param name="offset">The zero-based offset of the table whose type to return.</param>
		/// <returns>
		/// Returns a <see cref="String"/> that describes the type of table at the given offset.
		/// </returns>
		/// <exception cref="ArgumentOutOfRangeException">
		/// If the given <paramref name="offset"/> is smaller than 0 or greater or equal than
		/// <see cref="TableCount"/>.
		/// </exception>
		string GetTableType(int offset);

		/// <summary>
		/// Checks if a table with the given name is contained in the current context.
		/// </summary>
		/// <param name="name">The name of the table to search.</param>
		/// <returns>
		/// Returns <c>true</c> if any table with the given name was found in the container,
		/// <c>false</c> otherwise.
		/// </returns>
		bool ContainsTable(ObjectName name);

		/// <summary>
		/// Gets the table contained at the given offset within the context.
		/// </summary>
		/// <param name="offset">The zero-based offset of the table to return.</param>
		/// <returns>
		/// Returns an instance of <see cref="ITable"/> that provides access to the backed informaion
		/// for an object provided by the context.
		/// </returns>
		/// <exception cref="ArgumentOutOfRangeException">
		/// If the given <paramref name="offset"/> is smaller than 0 or greater or equal than
		/// <see cref="TableCount"/>.
		/// </exception>
		ITable GetTable(int offset);
	}
}
