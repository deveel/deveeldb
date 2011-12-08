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
	/// This interface represents the source of data in a table.
	/// </summary>
	/// <remarks>
	/// This is an abstraction that is used to Read data from within a table.
	/// <para>
	/// The entire contents of a table can be completely represented by 
	/// implementations of this interface.
	/// </para>
	/// </remarks>
	public interface ITableDataSource {
		/// <summary>
		/// Returns the <see cref="TransactionSystem"/> object that 
		/// describes global properties about the data source that 
		/// generated this object.
		/// </summary>
		TransactionSystem System { get; }

		/// <summary>
		/// Returns a <see cref="TableInfo"/> object that defines 
		/// the layout of the table that this data is in.
		/// </summary>
		/// <value>
		/// This may return 'null' if there is no table definition.
		/// </value>
		DataTableDef TableInfo { get; }

		///<summary>
		/// Returns the number of rows in this data source.
		///</summary>
		/// <remarks>
		/// Returns 'n' - <see cref="GetCellContents"/> is not necessarily valid for 
		/// row = [0..n]. Use <see cref="GetRowEnumerator"/> to generate an iterator 
		/// for valid row values over this data source.
		/// </remarks>
		int RowCount { get; }

		///<summary>
		/// Returns an iterator that is used to sequentually step through all valid 
		/// rows in this source.
		///</summary>
		/// <remarks>
		/// The iterator is guarenteed to return exactly <see cref="RowCount"/> elements. 
		/// The row elements returned by this iterator are used in <see cref="GetCellContents"/>
		/// in the <i>row</i> parameter.
		/// <para>
		/// This object is only defined if entries in the table are not added/remove during 
		/// the lifetime of this iterator. If entries are added or removed from the table 
		/// while this iterator is open, then calls to <see cref="IRowEnumerator.RowIndex"/>
		/// will be undefined.
		/// </para>
		/// </remarks>
		///<returns></returns>
		IRowEnumerator GetRowEnumerator();

		///<summary>
		/// Returns the <see cref="SelectableScheme"/> that we use as an index for 
		/// rows in the given column of this source.
		///</summary>
		///<param name="column"></param>
		/// <remarks>
		/// The SelectableScheme is used to determine the relationship between cells 
		/// in a column.
		/// <para>
		/// <b>Issue</b>: The scheme returned here should not have the <see cref="SelectableScheme.Insert"/>
		/// or <see cref="SelectableScheme.Remove"/> methods called (ie. it should be considered immutable).
		/// Perhaps we should make a <i>MutableSelectableScheme</i> interface to guarentee this constraint.
		/// </para>
		/// </remarks>
		///<returns></returns>
		SelectableScheme GetColumnScheme(int column);

		///<summary>
		/// Returns an object that represents the information in the given cell in the table.
		///</summary>
		///<param name="column"></param>
		///<param name="row"></param>
		/// <remarks>
		/// This may be an expensive operation, so calls to it should be kept to a minimum. 
		/// The offset between two rows is not necessarily 1. Use <see cref="GetRowEnumerator"/>
		/// to create a row iterator.
		/// </remarks>
		///<returns></returns>
		TObject GetCellContents(int column, int row);

	}
}