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
	///<summary>
	/// An object that encapsulates all row modification information about a 
	/// table when a change to the table is about to be committed.
	///</summary>
	/// <remarks>
	/// The object provides information about what rows in the table were changed
	/// (inserted/updated/deleted).
	/// </remarks>
	public class TableCommitModificationEvent {
		/// <summary>
		/// A SimpleTransaction that can be used to query tables in the database -
		/// the view of which will be the view when the transaction is committed.
		/// </summary>
		private readonly SimpleTransaction transaction;

		/// <summary>
		/// The name of the table that is being changed.
		/// </summary>
		private readonly TableName table_name;

		/// <summary>
		/// A normalized list of all rows that were added by the transaction 
		/// being committed.
		/// </summary>
		private readonly int[] added_rows;

		/// <summary>
		/// A normalized list of all rows that were removed by the transaction 
		/// being committed.
		/// </summary>
		private readonly int[] removed_rows;

		///<summary>
		///</summary>
		///<param name="transaction"></param>
		///<param name="table_name"></param>
		///<param name="added"></param>
		///<param name="removed"></param>
		public TableCommitModificationEvent(SimpleTransaction transaction,
								TableName table_name, int[] added, int[] removed) {
			this.transaction = transaction;
			this.table_name = table_name;
			added_rows = added;
			removed_rows = removed;
		}

		/// <summary>
		/// Returns the Transaction that represents the view of the database when
		/// the changes to the table have been committed.
		/// </summary>
		public SimpleTransaction Transaction {
			get { return transaction; }
		}

		/// <summary>
		/// Returns the name of the table.
		/// </summary>
		public TableName TableName {
			get { return table_name; }
		}

		///<summary>
		/// Returns the normalized list of all rows that were inserted or updated
		/// in this table of the transaction being committed.
		///</summary>
		/// <remarks>
		/// This is a normalized list which means if a row is inserted and then deleted 
		/// in the transaction then it is not considered important and does not appear 
		/// in this list.
		/// </remarks>
		public int[] AddedRows {
			get { return added_rows; }
		}


		///<summary>
		/// Returns the normalized list of all rows that were deleted or updated 
		/// in this table of the transaction being committed.
		///</summary>
		/// <remarks>
		/// This is a normalized list which means if a row is inserted and then deleted 
		/// in the transaction then it is not considered important and does not appear 
		/// in this list.
		/// </remarks>
		public int[] RemovedRows {
			get { return removed_rows; }
		}
	}
}