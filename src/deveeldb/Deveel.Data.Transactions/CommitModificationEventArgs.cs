// 
//  Copyright 2011  Deveel
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

namespace Deveel.Data.Transactions {
	///<summary>
	/// An object that encapsulates all row modification information about a 
	/// table when a change to the table is about to be committed.
	///</summary>
	/// <remarks>
	/// The object provides information about what rows in the table were changed
	/// (inserted/updated/deleted).
	/// </remarks>
	public sealed class CommitModificationEventArgs : EventArgs {
		/// <summary>
		/// The name of the table that is being changed.
		/// </summary>
		private readonly TableName tableName;

		/// <summary>
		/// A normalized list of all rows that were added by the transaction 
		/// being committed.
		/// </summary>
		private readonly int[] addedRows;

		/// <summary>
		/// A normalized list of all rows that were removed by the transaction 
		/// being committed.
		/// </summary>
		private readonly int[] removedRows;

		internal CommitModificationEventArgs(TableName tableName, int[] addedRows, int[] removedRows) {
			this.tableName = tableName;
			this.addedRows = addedRows;
			this.removedRows = removedRows;
		}

		/// <summary>
		/// Returns the name of the table.
		/// </summary>
		public TableName TableName {
			get { return tableName; }
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
			get { return addedRows; }
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
			get { return removedRows; }
		}
	}

	///<summary>
	/// A listener that is notified of table modification events made by a
	/// transaction, both immediately inside a transaction and when a 
	/// transaction commits.
	///</summary>
	/// <remarks>
	/// These events can occur either immediately before or immediately
	/// after the data is modified or during a commit.
	/// <para>
	/// This event occurs after constraint checks, and before the change is 
	/// actually committed to the database.  If this method generates an 
	/// exception then the change is rolled back and any changes made by the 
	/// transaction are lost.  This action is generated inside a 'commit Lock' 
	/// of the conglomerate, and therefore care should be taken with the 
	/// performance of this method.
	/// </para>
	/// <para>
	/// The event object provides access to a <see cref="SimpleTransaction"/>
	/// object that is a read-only view of the database in its committed state 
	/// (if this operation is successful). The transaction can be used to perform 
	/// any last minute deferred constraint checks.
	/// </para>
	/// <para>
	/// This action is useful for last minute abortion of a transaction, or for
	/// updating cache information.  It can not be used as a triggering mechanism
	/// and should never call back to user code.
	/// </para>
	/// </remarks>
	public delegate void CommitModificationEventHandler(SimpleTransaction sender, CommitModificationEventArgs args);
}