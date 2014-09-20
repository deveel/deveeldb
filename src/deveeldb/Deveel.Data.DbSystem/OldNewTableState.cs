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
namespace Deveel.Data.DbSystem {
	/// <summary>
	/// An internal table info object that handles OLD and NEW tables for
	/// triggered actions.
	/// </summary>
	public sealed class OldNewTableState {
		public OldNewTableState(TableName tableSource, int oldRowIndex, DataRow newDataRow, bool newMutable) {
			TableSource = tableSource;
			OldRowIndex = oldRowIndex;
			NewDataRow = newDataRow;
			IsNewMutable = newMutable;
		}

		internal OldNewTableState() {
			OldRowIndex = -1;
		}

		/// <summary>
		///  The name of the table that is the trigger source.
		/// </summary>
		public TableName TableSource { get; private set; }

		/// <summary>
		/// The row index of the OLD data that is being updated or deleted in the
		/// trigger source table.
		/// </summary>
		public int OldRowIndex { get; private set; }

		/// <summary>
		/// The DataRow of the new data that is being inserted/updated in the trigger
		/// source table.
		/// </summary>
		public DataRow NewDataRow { get; private set; }

		/// <summary>
		/// If true then the 'new_data' information is mutable which would be true for
		/// a BEFORE trigger.
		/// </summary>
		/// <remarks>
		/// For example, we would want to change the data in the row that caused the 
		/// trigger to fire.
		/// </remarks>
		public bool IsNewMutable { get; private set; }

		/// <summary>
		/// The DataTable object that represents the OLD table, if set.
		/// </summary>
		public DataTable OldDataTable { get; set; }

		/// <summary>
		/// The DataTable object that represents the NEW table, if set.
		/// </summary>
		public DataTable NewDataTable { get; set; }
	}
}