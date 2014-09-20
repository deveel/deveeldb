// 
//  Copyright 2010-2014 Deveel
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
using System.IO;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// An interface used for the inspection and repair of the raw data
	/// in a file.
	/// </summary>
	/// <remarks>
	/// Mainly used for table debugging and the repair of damaged
	/// files.
	/// </remarks>
	public interface IRawDiagnosticTable {
		/// <summary>
		/// Returns the number of physical records in the table.
		/// </summary>
		/// <remarks>
		/// The count includes records that are uncommitted, deleted, committed 
		/// removed and committed added.
		/// </remarks>
		int PhysicalRecordCount { get; }

		/// <summary>
		/// Gets the table meta informations describing the logical topology
		/// of the columns in the underyling table.
		/// </summary>
		DataTableInfo TableInfo { get; }

		/// <summary>
		/// Gets the state of the record at the given index in the table.
		/// </summary>
		/// <param name="recordIndex">The index of the record to get the state.</param>
		/// <returns></returns>
		RecordState GetRecordState(int recordIndex);

		/// <summary>
		/// Gets the size of the record at the given index in the table
		/// </summary>
		/// <param name="record_index">The index of the record to get the size.</param>
		/// <returns>
		/// Returns the number of bytes used by the given record on the underlying
		/// media.
		/// </returns>
		int GetRecordSize(int record_index);

		/// <summary>
		/// Returns the contents of the given cell in this table.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="record_index"></param>
		/// <returns></returns>
		/// <exception cref="IOException">If the system is unable 
		/// to return a valid cell value.</exception>
		TObject GetCellContents(int column, int record_index);

		/// <summary>
		/// Returns any miscellaneous information regarding the record at the given index
		/// in a human readable format.
		/// </summary>
		/// <param name="record_index"></param>
		/// <returns>
		/// Returns a string containing miscellaneous informations associated
		/// to the given record, or <b>null</b> if none was found.
		/// </returns>
		String GetRecordMiscInformation(int record_index);

	}
}