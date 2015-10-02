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
using System.Collections.Generic;

using Deveel.Data;
using Deveel.Data.Index;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Defines the contract to access the data contained
	/// into a table of a database.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Default implementations of this interface are set to be
	/// read-only: to make the table accessible in write-mode,
	/// tables will implement <see cref="IMutableTable"/>.
	/// </para>
	/// <para>
	/// A table rows can be enumerated in a sequence mode by
	/// using the <see cref="IEnumerable{Row}"/>, but if the
	/// table is altered the enumeration will throw an exception.
	/// </para>
	/// </remarks>
	public interface ITable : IDbObject, IEnumerable<Row>, IDisposable {
		IDatabaseContext DatabaseContext { get; }

		/// <summary>
		/// Gets the metadata information of the table, used to
		/// resolve the column sources.
		/// </summary>
		TableInfo TableInfo { get; }

		/// <summary>
		/// Gets the total number of rows in the table.
		/// </summary>
		int RowCount { get; }

		/// <summary>
		/// Gets a single cell within the table that is
		/// located at the given column offset and row.
		/// </summary>
		/// <param name="rowNumber">The unique number of the row 
		/// where the cell is located.</param>
		/// <param name="columnOffset">The zero-based offset of the 
		/// column of the cell to return.</param>
		/// <returns>
		/// Returns an instance of <see cref="DataObject"/> that is
		/// contained in the cell located by the row and column
		/// coordinates provided.
		/// </returns>
		/// <exception cref="ArgumentOutOfRangeException">
		/// If the given <paramref name="columnOffset"/> is less
		/// than zero or greater or equal than the number of columns
		/// defined in the table metadata.
		/// </exception>
		/// <seealso cref="Sql.TableInfo.IndexOfColumn(string)"/>
		DataObject GetValue(long rowNumber, int columnOffset);

		/// <summary>
		/// Gets an index for given column that can be used to select
		/// values from this table.
		/// </summary>
		/// <param name="columnOffset">The zero-based offset of the column
		/// which to get the index.</param>
		/// <returns>
		/// Returns an instance of <see cref="ColumnIndex"/> that is used to
		/// select a subset of rows from the table.
		/// </returns>
		ColumnIndex GetIndex(int columnOffset);
	}
}