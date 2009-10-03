//
//  This file is part of DeveelDB.
//
//    DeveelDB is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Lesser General Public License as 
//    published by the Free Software Foundation, either version 3 of the 
//    License, or (at your option) any later version.
//
//    DeveelDB is distributed in the hope that it will be useful,
//    but WITHOUT ANY WARRANTY; without even the implied warranty of
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//    GNU Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public 
//    License along with DeveelDB.  If not, see <http://www.gnu.org/licenses/>.
//
//  Authors:
//    Antonello Provenzano <antonello@deveel.com>
//    Tobias Downer <toby@mckoi.com>
//

using System;
using System.Collections;

namespace Deveel.Data {
	/// <summary>
	/// A <see cref="IRowEnumerator"/> implementation that represents 
	/// a sequence of rows that can be referenced in incremental order between 0 
	/// and the row count of the table.
	/// </summary>
	/// <remarks>
	/// A table that returns a <see cref="SimpleRowEnumerator"/> is guarenteed 
	/// to provide valid <see cref="TObject"/> values via the 
	/// <see cref="Table.GetCellContents"/> method between rows 0 and 
	/// <see cref="Table.RowCount"/>.
	/// </remarks>
	public sealed class SimpleRowEnumerator : IRowEnumerator {
		/// <summary>
		/// The current index.
		/// </summary>
		private int index = -1;
		/// <summary>
		/// The number of rows in the enumeration.
		/// </summary>
		readonly int row_count_store;

		///<summary>
		///</summary>
		///<param name="row_count"></param>
		public SimpleRowEnumerator(int row_count) {
			row_count_store = row_count;
		}

		/// <inheritdoc/>
		public bool MoveNext() {
			return (++index < row_count_store);
		}

		/// <inheritdoc/>
		public void Reset() {
			index = -1;
		}

		object IEnumerator.Current {
			get { return RowIndex; }
		}

		//TODO: check this...
		/// <inheritdoc/>
		public int RowIndex {
			get { return index; }
		}
	}
}