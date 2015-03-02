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
using System.Collections;

namespace Deveel.Data.DbSystem {
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