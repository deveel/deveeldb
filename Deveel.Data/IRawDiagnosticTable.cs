// 
//  IRawDiagnosticTable.cs
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
using System.IO;

namespace Deveel.Data {
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
		DataTableDef DataTableDef { get; }

		/// <summary>
		/// Gets the state of the record at the given index in the table.
		/// </summary>
		/// <param name="record_index">The index of the record to get the state.</param>
		/// <returns></returns>
		RecordState GetRecordState(int record_index);

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