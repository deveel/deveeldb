//  
//  TableCommitModificationEvent.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

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