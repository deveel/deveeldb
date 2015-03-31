// 
//  Copyright 2010-2015 Deveel
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
//

using System;

namespace Deveel.Data.Sql {
	/// <summary>
	/// An interface that defines contracts to alter the
	/// contents of a table.
	/// </summary>
	public interface IMutableTable : ITable {
		/// <summary>
		/// Creates a new row that is compatible with the 
		/// table context, ready to be populated and added.
		/// </summary>
		/// <remarks>
		/// <para>
		/// When this method is called, a new <see cref="RowId"/>
		/// is generated and persisted: when a subsequent call to
		/// this method will be issued, another new row identifier
		/// will be generated, even if the row was not persisted
		/// into the table.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="Row"/> that
		/// belongs to this table and can be added through
		/// <see cref="AddRow"/> call.
		/// </returns>
		Row NewRow();

		/// <summary>
		/// Persists a new row to the table.
		/// </summary>
		/// <remarks>
		/// <para>
		/// The row to be added must have been generated through
		/// <see cref="NewRow"/> factory method, otherwise an
		/// exception will be thrown.
		/// </para>
		/// </remarks>
		/// <param name="row">The row to be persisted.</param>
		/// <exception cref="ArgumentException">
		/// If the gven <paramref name="row"/> was not generated using
		/// <see cref="NewRow"/> factory method.
		/// </exception>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="row"/> is <c>null</c>.
		/// </exception>
		void AddRow(Row row);

		/// <summary>
		/// Updates the values of a row into the table.
		/// </summary>
		/// <param name="row">The object containing the values to update.</param>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="row"/> is <c>null</c>.
		/// </exception>
		void UpdateRow(Row row);

		/// <summary>
		/// Deletes row identified by the given coordinates from the table.
		/// </summary>
		/// <param name="rowId">The unique identifier of the row to be removed.</param>
		/// <returns>
		/// Returns <c>true</c> if the row identified was found and removed,
		/// <c>false</c> otherwise.
		/// </returns>
		/// <exception cref="ArgumentException">
		/// Thrown if the given <paramref name="rowId"/> does not belong
		/// to this table.
		/// </exception>
		bool RemoveRow(RowId rowId);

		/// <summary>
		/// Flushes all changes made on this table to the backing
		/// index scheme.
		/// </summary>
		/// <remarks>
		/// <para>
		/// This is used during the commit phase of this objects lifetime.  
		/// The transaction control mechanism has found that there are no 
		/// clashes and now we need to commit the current table view to the
		/// conglomerate. Because this object may not update index information
		/// immediately, we call this to flush all the changes to the table to the
		/// backing index set.
		/// </para>
		/// <para>
		/// When this method returns, the backing index-set 
		/// of this view will be completely up to date.
		/// </para>
		/// </remarks>
		void FlushIndexes();

		/// <summary>
		/// Performs all constraint integrity checks and actions to any modifications
		/// based on any changes that happened to the table since that last call to
		/// this method.
		/// </summary>
		/// <remarks>
		/// <para>
		/// It is important that is called after any call to <see cref="AddRow"/>,
		/// <see cref="RemoveRow"/> or <see cref="UpdateRow"/>.
		/// </para>
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
		void AssertConstraints();

		/// <summary>
		/// Puts this source under a <c>root lock</c>.
		/// </summary>
		/// <remarks>
		/// <para>
		/// A root lock means the root row structure of this object must not change.  
		/// </para>
		/// <para>
		/// A root lock is obtained on a table when a result set keeps hold of an 
		/// object outside the life of the transaction that created the table.  
		/// </para>
		/// <para>
		/// It is important that the order of the rows stays constant (committed 
		/// deleted rows are not really deleted and reused, etc) while a table 
		/// holds at least 1 root lock.
		/// </para>
		/// </remarks>
		void AddLock();

		/// <summary>
		/// Release a <c>root lock</c> from this table.
		/// </summary>
		/// <remarks>
		/// <para>
		/// When all the locks to a table have been removed, the structure
		/// can be altered.
		/// </para>
		/// </remarks>
		/// <seealso cref="AddLock"/>
		void RemoveLock();
	}
}