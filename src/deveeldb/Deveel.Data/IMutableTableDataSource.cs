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
using System.IO;

namespace Deveel.Data {
	/// <summary>
	/// A mutable data source that allows for the addition and 
	/// removal of rows.
	/// </summary>
	public interface IMutableTableDataSource : ITableDataSource, IDisposable {
		/// <summary>
		/// Returns a journal that details the changes to this data source 
		/// since it was created.
		/// </summary>
		/// <remarks>
		/// This method may return a <b>null</b> object to denote that no
		/// logging is being done. If this returns a <see cref="MasterTableJournal"/>, 
		/// then all <see cref="AddRow"/> and <see cref="RemoveRow"/> method calls and their relative
		/// order will be described in this journal.
		/// </remarks>
		MasterTableJournal Journal { get; }


		/// <summary>
		/// Adds a row to the source.
		/// </summary>
		/// <param name="dataRow">The row to add.</param>
		/// <remarks>
		/// This will add a permanent record into the the underlying data 
		/// structure. It will also update the indexing schemes as appropriate, 
		/// and also add the row into the set returned by the enumerator
		/// returned by <see cref="ITableDataSource.GetRowEnumerator"/>.
		/// </remarks>
		/// <returns>
		/// Returns a row index that is used to reference this data in 
		/// future queries.
		/// </returns>
		/// <exception cref="IOException">
		/// If the row addition was not possible because of IO reasons.
		/// </exception>
		int AddRow(DataRow dataRow);

		/// <summary>
		/// Completely removes a row from the source.
		/// </summary>
		/// <param name="rowIndex">Index of the row to remove.</param>
		/// <remarks>
		/// This will permanently remove the record from the underlying data 
		/// structure. It also updates the indexing schemes and removes the 
		/// row index from the set returned by the <see cref="ITableDataSource.GetRowEnumerator"/>
		/// enumerator.
		/// </remarks>
		/// <exception cref="DatabaseException">
		/// If the row index does not reference a valid row within the context
		/// of this data source.
		/// </exception>
		void RemoveRow(int rowIndex);

		/// <summary>
		/// Updates a row in the source.
		/// </summary>
		/// <param name="rowIndex">Index of the row to update.</param>
		/// <param name="dataRow">Row data to update.</param>
		/// <remarks>
		/// This will make a permanent change to the underlying data structure.
		/// It will update the indexing schemes as appropriate, and also add the row 
		/// into the set returned by the <see cref="ITableDataSource.GetRowEnumerator"/> 
		/// enumerator.
		/// </remarks>
		/// <returns>Returns a row index for the new updated records.</returns>
		/// <exception cref="IOException">
		/// If the row update was not possible 
		/// because of IO reasons</exception>
		/// <exception cref="DatabaseException">
		/// If the row index not being a valid reference to a record in this 
		/// data source.
		/// </exception>
		int UpdateRow(int rowIndex, DataRow dataRow);

		/// <summary>
		/// Flushes all changes made on this table to the backing
		/// index scheme (IIndexSet).
		/// </summary>
		/// <remarks>
		/// This is used during the commit phase of this objects lifetime.  
		/// The transaction control mechanism has found that there are no 
		/// clashes and now we need to commit the current table view to the
		/// conglomerate. Because this object may not update index information
		/// immediately, we call this to flush all the changes to the table to the
		/// backing index set.
		/// <para>
		/// When this method returns, the backing <see cref="IIndexSet"/> 
		/// of this view will be completely up to date.
		/// </para>
		/// </remarks>
		void FlushIndexChanges();

		/// <summary>
		/// Performs all constraint integrity checks and actions to any modifications
		/// based on any changes that happened to the table since that last call to
		/// this method.
		/// </summary>
		/// <remarks>
		/// It is important that is called after any call to <see cref="AddRow"/>,
		/// <see cref="RemoveRow"/> or <see cref="UpdateRow"/>.
		/// <para>
		/// Any constraints that are marked as <see cref="ConstraintDeferrability.InitiallyImmediate"/>
		/// are checked when this is called, otherwise the constraint is checked 
		/// at commit time.
		/// </para>
		/// <para>
		/// Any referential actions are performed when this method is called. If a
		/// referential action causes a modification to another table, this method
		/// is recursively called on the table modified.
		/// </para>
		/// <para>
		/// If a referential integrity constraint is violated and a referential action
		/// is unable to maintain the integrity of the database, any changes made to
		/// the table are reverted.
		/// </para>
		/// </remarks>
		void ConstraintIntegrityCheck();

		/// <summary>
		/// Puts this source under a 'root lock'.
		/// </summary>
		/// <remarks>
		/// A root lock means the root row structure of this object must not change.  
		/// A root lock is obtained on a table when a result set keeps hold of an 
		/// object outside the life of the transaction that created the table.  
		/// It is important that the order of the rows stays constant (committed 
		/// deleted rows are not really deleted and reused, etc) while a table 
		/// holds at least 1 root lock.
		/// </remarks>
		void AddRootLock();

		/// <summary>
		/// Removes a root lock from this source (see <seealso cref="AddRootLock"/>).
		/// </summary>
		void RemoveRootLock();

	}
}