//  
//  DataTable.cs
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

using Deveel.Data.Collections;
using Deveel.Diagnostics;

using SysMath = System.Math;

namespace Deveel.Data {
	/// <summary>
	/// Represents a wrapper for a <see cref="IMutableTableDataSource"/> 
	/// that fits into the query hierarchy level.
	/// </summary>
	/// <remarks>
	/// A DataTable represents a table within a transaction. Adding, removing 
	/// rows to this table will change the contents only with the context of the 
	/// transaction the table was created in.
	/// </remarks>
	public sealed class DataTable : DefaultDataTable {
		/// <summary>
		/// The DatabaseConnection object that is the parent of this DataTable.
		/// </summary>
		private readonly DatabaseConnection connection;

		/// <summary>
		/// A low level access to the underlying transactional data source.
		/// </summary>
		private readonly IMutableTableDataSource data_source;


		/**
		 * ------
		 * NOTE: Following values are only kept for Lock debugging reasons.  These
		 *   is no technical reason why they shouldn't be removed.  They allow us
		 *   to check that a data table is locked correctly when accesses are
		 *   performed on it.
		 * ------
		 */

		const bool LOCK_DEBUG = true;

		/// <summary>
		/// The number of read locks we have on this table.
		/// </summary>
		private int debug_read_lock_count = 0;

		/// <summary>
		/// The number of write locks we have on this table (this should 
		/// only ever be 0 or 1).
		/// </summary>
		private int debug_write_lock_count = 0;


		internal DataTable(DatabaseConnection connection,
				  IMutableTableDataSource data_source)
			: base(connection.Database) {
			this.connection = connection;
			this.data_source = data_source;
		}

		/*
		TODO:
		public override IDebugLogger Debug {
			get { return connection.System.Debug; }
		}
		*/

		/// <inheritdoc/>
		protected override void BlankSelectableSchemes(int type) {
		}

		/// <inheritdoc/>
		protected override SelectableScheme GetRootColumnScheme(int column) {
			CheckReadLock();  // Read op

			return data_source.GetColumnScheme(column);
		}

		/// <summary>
		/// Declares the table as a new type.
		/// </summary>
		/// <param name="new_name">The name of the declared table.</param>
		/// <returns>
		/// Returns a <see cref="ReferenceTable"/> representing the new 
		/// declaration of the table.
		/// </returns>
		public ReferenceTable DeclareAs(TableName new_name) {
			return new ReferenceTable(this, new_name);
		}

		/// <summary>
		/// Generates a new row for the addition of data to the table.
		/// </summary>
		/// <param name="context">The context of the row.</param>
		/// <remarks>
		/// To add the data setted to the resultant <see cref="RowData"/>
		/// object it must be passed to the <see cref="AddRow"/> method.
		/// </remarks>
		/// <returns>
		/// Returns a <see cref="RowData"/> representing a row for the addition 
		/// of data to the table.
		/// </returns>
		public RowData createRowDataObject(IQueryContext context) {
			CheckSafeOperation();  // safe op
			return new RowData(this);
		}

		/// <inheritdoc/>
		public override int RowCount {
			get {
				CheckReadLock(); // Read op

				return data_source.RowCount;
			}
		}

		/**
		 * Adds a given 'RowData' object to the table.  This should be used for
		 * any rows added to the table.  The order that rows are added into a table
		 * is not important.
		 * <p>
		 * This method performs some checking of the cells in the table.  It first
		 * checks that all columns declared as 'not null' have a value that is not
		 * null.  It then checks that a the added row will not cause any duplicates
		 * in a column declared as unique.
		 * <p>
		 * It then uses the low level io manager to store the data.
		 * <p>
		 * SYNCHRONIZATION ISSUE: We are assuming this is running in a synchronized
		 *   environment that is unable to add or alter rows in this object within
		 *   the lifetime of this method.
		 */
		public void Add(RowData row_data) {
			CheckReadWriteLock();  // Write op

			if (!row_data.IsSameTable(this)) {
				throw new DatabaseException(
							   "Internal Error: Using RowData from different table");
			}

			// Checks passed, so add to table.
			AddRow(row_data);

			// Perform a referential integrity check on any changes to the table.
			data_source.ConstraintIntegrityCheck();
		}

		/// <summary>
		/// Adds an array of new rows to the table.
		/// </summary>
		/// <param name="row_data_arr">The array of rows to add.</param>
		/// <remarks>
		/// This should be used for any rows added to the table. The order 
		/// that rows are added into a table is not important.
		/// <para>
		/// This method performs some checking of the cells in the table:
		/// <list type="number">
		/// <item>
		/// Checks that all columns declared as 'not null' have a value that 
		/// is not null.
		/// </item>
		/// <item>
		/// Checks that a the added row will not cause any duplicates
		/// in a column declared as unique.
		/// </item>
		/// <item>
		/// Uses the low level io manager to store the data.
		/// </item>
		/// </list>
		/// </para>
		/// <para>
		/// <b>Locking Issue:</b> We are assuming this is running in a <i>lock</i>
		/// environment that is unable to add or alter rows in this object within
		/// the lifetime of this method.
		/// </para>
		/// </remarks>
		/// <exception cref="DatabaseException">
		/// If the any of the given <paramref name="row_data_arr"/> was generated by 
		/// another data table.
		/// </exception>
		public void Add(RowData[] row_data_arr) {
			CheckReadWriteLock();  // Write op

			for (int i = 0; i < row_data_arr.Length; ++i) {
				RowData row_data = row_data_arr[i];
				if (!row_data.IsSameTable(this)) {
					throw new DatabaseException(
								   "Internal Error: Using RowData from different table");
				}
				AddRow(row_data);
			}

			// Perform a referential integrity check on any changes to the table.
			data_source.ConstraintIntegrityCheck();
		}

		/// <summary>
		/// Adds a new row of data to the table.
		/// </summary>
		/// <param name="row"></param>
		/// <remarks>
		/// First of all, this tells the underlying database mechanism to 
		/// add the data to this table.  It then add the row information to 
		/// each SelectableScheme.
		/// </remarks>
		private void AddRow(RowData row) {

			// This table name (for event notification)
			TableName table_name = TableName;

			// Fire the 'before' trigger for an insert on this table
			connection.FireTableEvent(new TableModificationEvent(connection, table_name,
																 row, true));

			// Add the row to the underlying file system
			int row_number = data_source.AddRow(row);

			// Fire the 'after' trigger for an insert on this table
			connection.FireTableEvent(new TableModificationEvent(connection, table_name,
																 row, false));

			// NOTE: currently nothing being done with 'row_number' after it's added.
			//   The underlying table data source manages the row index.

		}

		/// <summary>
		/// Removes the given row from the table.
		/// </summary>
		/// <param name="row_number">The index of the row to delete.</param>
		/// <remarks>
		/// This is called just before the row is actually deleted. The method 
		/// is provided to allow for some maintenance of any search structures 
		/// such as B-Trees.
		/// This is called from the <see cref="Delete(Deveel.Data.Table)"/>.
		/// </remarks>
		private void RemoveRow(int row_number) {

			// This table name (for event notification)
			TableName table_name = TableName;

			// Fire the 'before' trigger for the delete on this table
			connection.FireTableEvent(new TableModificationEvent(connection, table_name,
																 row_number, true));

			// Delete the row from the underlying database
			data_source.RemoveRow(row_number);

			// Fire the 'after' trigger for the delete on this table
			connection.FireTableEvent(new TableModificationEvent(connection, table_name,
																 row_number, false));

		}

		/// <summary>
		/// Updates the given row with the given data in this table.
		/// </summary>
		/// <param name="row_number"></param>
		/// <param name="row"></param>
		/// <remarks>
		/// This method will likely add the modified data to a new row and 
		/// delete the old version of the row.
		/// </remarks>
		private void UpdateRow(int row_number, RowData row) {

			// This table name (for event notification)
			TableName table_name = TableName;

			// Fire the 'before' trigger for the update on this table
			connection.FireTableEvent(
				 new TableModificationEvent(connection, table_name,
											row_number, row, true));

			// Update the row in the underlying database
			data_source.UpdateRow(row_number, row);

			// Fire the 'after' trigger for the update on this table
			connection.FireTableEvent(
				 new TableModificationEvent(connection, table_name,
											row_number, row, false));

		}

		/// <summary>
		/// Removes from the table any row that are in the given table.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="limit">The maximum number of rows to delete (if less 
		/// than zero, no limit).</param>
		/// <remarks>
		/// <b>Internal Note:</b> <paramref name="table"/> may be the result
		/// of joins. This may cause the same row in this table to be referenced
		/// more than once. We must make sure that we delete any given row only
		/// once by using the <i>distinct</i> function.
		/// <para>
		/// Keep in mind that rows are picked out from top to bottom in the 'table' 
		/// object. Normally the input table will be the result of an un-ordered 
		/// <i>where</i> clause so using a limit does not permit deletes in a 
		/// deterministic manner.
		/// </para>
		/// <para>
		/// <b>Assumption:</b> There are no duplicate rows in the input set.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns the number of rows that were deleted.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If the current table is not a distant ancestoir of the given 
		/// <paramref name="table"/>.
		/// </exception>
		public int Delete(Table table, int limit) {
			CheckReadWriteLock();  // Write op

			IntegerVector row_set = new IntegerVector(table.RowCount);
			IRowEnumerator e = table.GetRowEnumerator();
			while (e.MoveNext()) {
				row_set.AddInt(e.RowIndex);
			}
			e = null;

			// HACKY: Find the first column of this table in the search table.  This
			//   will allow us to generate a row set of only the rows in the search
			//   table.
			int first_column = table.FindFieldName(GetResolvedVariable(0));

			if (first_column == -1) {
				throw new DatabaseException("Search table does not contain any " +
											"reference to table being deleted from");
			}

			// Generate a row set that is in this tables domain.
			table.SetToRowTableDomain(first_column, row_set, this);

			// row_set may contain duplicate row indices, therefore we must sort so
			// any duplicates are grouped and therefore easier to find.
			row_set.QuickSort();

			// If limit less than zero then limit is whole set.
			if (limit < 0) {
				limit = Int32.MaxValue;
			}

			// Remove each row in row set in turn.  Make sure we don't remove the
			// same row index twice.
			int len = SysMath.Min(row_set.Count, limit);
			int last_removed = -1;
			int remove_count = 0;
			for (int i = 0; i < len; ++i) {
				int to_remove = row_set[i];
				if (to_remove < last_removed) {
					throw new DatabaseException(
					  "Internal error: row sorting error or row_set not in the range > 0");
				}

				if (to_remove != last_removed) {
					RemoveRow(to_remove);
					last_removed = to_remove;
					++remove_count;
				}

			}

			if (remove_count > 0) {
				// Perform a referential integrity check on any changes to the table.
				data_source.ConstraintIntegrityCheck();
			}

			return remove_count;
		}

		/// <summary>
		/// Removes from the table any row that are in the given table.
		/// </summary>
		/// <param name="table"></param>
		/// <remarks>
		/// <b>Internal Note:</b> <paramref name="table"/> may be the result
		/// of joins. This may cause the same row in this table to be referenced
		/// more than once. We must make sure that we delete any given row only
		/// once by using the <i>distinct</i> function.
		/// <para>
		/// Keep in mind that rows are picked out from top to bottom in the 'table' 
		/// object. Normally the input table will be the result of an un-ordered 
		/// <i>where</i> clause so using a limit does not permit deletes in a 
		/// deterministic manner.
		/// </para>
		/// <para>
		/// <b>Assumption:</b> There are no duplicate rows in the input set.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns the number of rows that were deleted.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If the current table is not a distant ancestoir of the given 
		/// <paramref name="table"/>.
		/// </exception>
		public int Delete(Table table) {
			return Delete(table, -1);
		}

		/// <summary>
		/// Updates the table by applying the assignment operations over each row
		/// that is found in the input table set.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="table"></param>
		/// <param name="assign_list"></param>
		/// <param name="limit">The maximum number of rows to update (if less
		/// than zero, no limit).</param>
		/// <remarks>
		/// The input table must be a direct child of the current table.
		/// <para>
		/// This operation assumes that there is a <b>write</b> lock on this table. 
		/// A <b>write</b> lock means no other thread may access this table while the
		/// operation is being performed. (However, a result set may still be
		/// downloading from this table).
		/// </para>
		/// <para>
		/// Keep in mind that rows are picked out from top to bottom in the <paramref name="table"/>. 
		/// Normally the input table will be the result of an un-ordered 
		/// <i>where</i> clause so using a limit does not permit updates in a 
		/// deterministic manner.
		/// </para>
		/// <para>
		/// <b>Note:</b> We assume there are no duplicate rows to the root set from the
		/// given <paramref name="table"/>.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns the number of rows updated in this table.
		/// </returns>
		public int Update(IQueryContext context, Table table, Assignment[] assign_list, int limit) {
			CheckReadWriteLock();  // Write op

			// Get the rows from the input table.
			IntegerVector row_set = new IntegerVector();
			IRowEnumerator e = table.GetRowEnumerator();
			while (e.MoveNext()) {
				row_set.AddInt(e.RowIndex);
			}
			e = null;

			// HACKY: Find the first column of this table in the search table.  This
			//   will allow us to generate a row set of only the rows in the search
			//   table.
			int first_column = table.FindFieldName(GetResolvedVariable(0));
			if (first_column == -1) {
				throw new DatabaseException("Search table does not contain any " +
											"reference to table being updated from");
			}

			// Convert the row_set to this table's domain.
			table.SetToRowTableDomain(first_column, row_set, this);

			// NOTE: Assume there's no duplicate rows.

			RowData original_data = createRowDataObject(context);
			RowData row_data = createRowDataObject(context);

			// If limit less than zero then limit is whole set.
			if (limit < 0) {
				limit = Int32.MaxValue;
			}

			// Update each row in row set in turn up to the limit.
			int len = SysMath.Min(row_set.Count, limit);
			int update_count = 0;
			for (int i = 0; i < len; ++i) {
				int to_update = row_set[i];

				// Make a RowData object from this row (plus keep the original intact
				// incase we need to roll back to it).
				original_data.SetFromRow(to_update);
				row_data.SetFromRow(to_update);

				// Run each assignment on the RowData.
				for (int n = 0; n < assign_list.Length; ++n) {
					Assignment assignment = assign_list[n];
					row_data.Evaluate(assignment, context);
				}

				// Update the row
				UpdateRow(to_update, row_data);

				++update_count;
			}

			if (update_count > 0) {
				// Perform a referential integrity check on any changes to the table.
				data_source.ConstraintIntegrityCheck();
			}

			return update_count;

		}

		/// <inheritdoc/>
		public override DataTableDef DataTableDef {
			get {
				CheckSafeOperation(); // safe op

				return data_source.DataTableDef;
			}
		}

		/// <summary>
		/// Returns the schema that this table is within.
		/// </summary>
		public string Schema {
			get {
				CheckSafeOperation(); // safe op

				return DataTableDef.Schema;
			}
		}

		/// <inheritdoc/>
		internal override void AddDataTableListener(IDataTableListener listener) {
			// Currently we do nothing with this info.
		}


		/// <inheritdoc/>
		internal override void RemoveDataTableListener(IDataTableListener listener) {
			// Currently we do nothing with this info.
		}




		// -------- Methods implemented for DefaultDataTable --------

		/// <inheritdoc/>
		internal override void SetToRowTableDomain(int column, IntegerVector row_set, ITableDataSource ancestor) {
			CheckReadLock();  // Read op

			if (ancestor != this && ancestor != data_source) {
				throw new Exception("Method routed to incorrect table ancestor.");
			}
		}

		/// <inheritdoc/>
		public override TObject GetCellContents(int column, int row) {
			CheckSafeOperation();  // safe op

			return data_source.GetCellContents(column, row);
		}

		/// <inheritdoc/>
		public override IRowEnumerator GetRowEnumerator() {
			CheckReadLock(); // Read op

			return data_source.GetRowEnumerator();
		}


		/// <inheritdoc/>
		public override void LockRoot(int lock_key) {
			CheckSafeOperation();  // safe op

			data_source.AddRootLock();
		}

		/// <inheritdoc/>
		public override void UnlockRoot(int lock_key) {
			CheckSafeOperation();  // safe op

			data_source.RemoveRootLock();
		}

		/// <inheritdoc/>
		public override bool HasRootsLocked {
			get {
				// There is no reason why we would need to know this information at
				// this level.
				// We need to deprecate this properly.
				throw new ApplicationException("hasRootsLocked is deprecated.");
			}
		}


		// ------------ Lock debugging methods ----------

		/// <summary>
		/// This is called by the <see cref="Lock"/> lass to notify this 
		/// <see cref="DataTable"/> that a read/write lock has been applied 
		/// to this table.
		/// </summary>
		/// <param name="lock_type"></param>
		/// <remarks>
		/// This is for Lock debugging purposes only.
		/// </remarks>
		internal void OnAddRWLock(AccessType lock_type) {
			if (LOCK_DEBUG) {
				if (lock_type == AccessType.Read) {
					++debug_read_lock_count;
				} else if (lock_type == AccessType.Write) {
					++debug_write_lock_count;
					if (debug_write_lock_count > 1) {
						throw new ApplicationException(">1 Write Lock on table " + TableName);
					}
				} else {
					throw new ApplicationException("Unknown Lock type: " + lock_type);
				}
			}
		}

		/// <summary>
		/// This is called by <see cref="Lock"/> to notify this table that a 
		/// read/write lock has been released from it.
		/// </summary>
		/// <param name="lock_type">The type of the lock released.</param>
		/// <remarks>
		/// This is for lock debugging purposes only.
		/// </remarks>
		internal void notifyReleaseRWLock(AccessType lock_type) {
			if (LOCK_DEBUG) {
				if (lock_type == AccessType.Read) {
					--debug_read_lock_count;
				} else if (lock_type == AccessType.Write) {
					--debug_write_lock_count;
				} else {
					Debug.WriteException(new Exception("Unknown Lock type: " + lock_type));
				}
			}
		}

		/// <summary>
		/// Returns true if the database is in exclusive mode.
		/// </summary>
		private bool IsInExclusiveMode {
			get {
				// Check the connection locking mechanism is in exclusive mode
				return connection.LockingMechanism.IsInExclusiveMode;
			}
		}

		/// <summary>
		/// Checks the database is in exclusive mode.
		/// </summary>
		private void CheckInExclusiveMode() {
			if (!IsInExclusiveMode) {
				Debug.WriteException(new Exception(
				   "Performed exclusive operation on table and not in exclusive mode!"));
			}
		}

		/// <summary>
		/// Check that we can safely Read from this table.
		/// </summary>
		private void CheckReadLock() {
			if (LOCK_DEBUG) {
				// All 'sUSR' tables are given Read access because they may only be
				// written under exclusive mode anyway.

				bool is_internal_table =
							  TableName.Schema.Equals(Database.SystemSchema);

				if (!(is_internal_table ||
					  debug_read_lock_count > 0 ||
					  debug_write_lock_count > 0 ||
					  IsInExclusiveMode)) {

					Console.Error.WriteLine();
					Console.Error.Write(" is_internal_table = " + is_internal_table);
					Console.Error.Write(" debug_read_lock_count = " + debug_read_lock_count);
					Console.Error.Write(" debug_write_lock_count = " + debug_write_lock_count);
					Console.Error.WriteLine(" isInExclusiveMode = " + IsInExclusiveMode);

					Debug.WriteException(new ApplicationException(
								"Invalid Read access on table '" + TableName + "'"));
				}
			}
		}

		/// <summary>
		/// Check that we can safely read/write from this table.
		/// </summary>
		/// <remarks>
		/// This should catch any synchronization concurrent issues.
		/// </remarks>
		private void CheckReadWriteLock() {
			if (LOCK_DEBUG) {
				// We have to own exactly one Write Lock, or be in exclusive mode.
				if (!(debug_write_lock_count == 1 || IsInExclusiveMode)) {
					Debug.WriteException(
						   new ApplicationException("Invalid Read/Write access on table '" +
									 TableName + "'"));
				}
			}
		}

		/// <summary>
		/// Check that we can run a safe operation.
		/// </summary>
		private void CheckSafeOperation() {
			// no operation - nothing to check for...
		}



		// ---------- Overwritten to output debug info ----------
		// NOTE: These can all safely be commented out.

		/// <inheritdoc/>
		public override int ColumnCount {
			get {
				CheckSafeOperation(); // safe op

				return base.ColumnCount;
			}
		}

		/// <inheritdoc/>
		public override Variable GetResolvedVariable(int column) {
			CheckSafeOperation();  // safe op

			return base.GetResolvedVariable(column);
		}

		/// <inheritdoc/>
		public override int FindFieldName(Variable v) {
			CheckSafeOperation();  // safe op

			return base.FindFieldName(v);
		}

		/// <inheritdoc/>
		internal override SelectableScheme GetSelectableSchemeFor(int column, int original_column, Table table) {
			CheckReadLock();  // Read op

			return base.GetSelectableSchemeFor(column, original_column, table);
		}

		/// <inheritdoc/>
		internal override RawTableInformation ResolveToRawTable(RawTableInformation info) {
			CheckReadLock();  // Read op

			return base.ResolveToRawTable(info);
		}
	}
}