// 
//  Copyright 2010-2016 Deveel
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

using Deveel.Data;

namespace Deveel.Data.Sql.Tables {
	/// <summary>
	/// An interface that defines contracts to alter the
	/// contents of a table.
	/// </summary>
	public interface IMutableTable : ITable {
		TableEventRegistry EventRegistry { get; }

		void AddLock();

		void RemoveLock();

		/// <summary>
		/// Persists a new row to the table.
		/// </summary>
		/// <remarks>
		/// <para>
		/// The row to be added must belong to the table context, 
		/// otherwise an exception will be thrown.
		/// </para>
		/// </remarks>
		/// <param name="row">The row to be persisted.</param>
		/// <returns>
		/// Returns a <see cref="RowId"/> that is the pointer to the row
		/// established in the table.
		/// </returns>
		/// <exception cref="ArgumentException">
		/// If the gven <paramref name="row"/> does not belong to 
		/// the table context.
		/// </exception>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="row"/> is <c>null</c>.
		/// </exception>
		RowId AddRow(Row row);

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
	}
}