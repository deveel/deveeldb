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
using System.Collections.Generic;

using Deveel.Data.Collections;
using Deveel.Diagnostics;

using SysMath = System.Math;

namespace Deveel.Data {
	/// <summary>
	/// Represents a wrapper for a <see cref="IMutableTableDataSource"/> 
	/// that fits into the command hierarchy level.
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
		private readonly IMutableTableDataSource dataSource;

#if DEBUG
		/// <summary>
		/// The number of read locks we have on this table.
		/// </summary>
		private int debugReadLockCount;

		/// <summary>
		/// The number of write locks we have on this table (this should 
		/// only ever be 0 or 1).
		/// </summary>
		private int debugWriteLockCount;
#endif

		internal DataTable(DatabaseConnection connection, IMutableTableDataSource dataSource)
			: base(connection.Database) {
			this.connection = connection;
			this.dataSource = dataSource;
		}

		/// <inheritdoc/>
		public override int RowCount {
			get {
				CheckReadLock(); // Read op
				return dataSource.RowCount;
			}
		}

		/// <inheritdoc/>
		public override DataTableInfo DataTableInfo {
			get {
				CheckSafeOperation(); // safe op
				return dataSource.DataTableInfo;
			}
		}

		/// <summary>
		/// Returns the schema that this table is within.
		/// </summary>
		public string Schema {
			get {
				CheckSafeOperation(); // safe op
				return DataTableInfo.Schema;
			}
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

		protected internal override IDebugLogger Debug {
			get { return connection.System.Debug; }
		}

		/// <inheritdoc/>
		public override int ColumnCount {
			get {
				CheckSafeOperation(); // safe op
				return base.ColumnCount;
			}
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
		private void AddRow(DataRow row) {
			// This table name (for event notification)
			TableName tableName = TableName;

			// Fire the 'before' trigger for an insert on this table
			connection.FireTableEvent(new TableModificationEvent(tableName, row, true));

			// Add the row to the underlying file system
			dataSource.AddRow(row);

			// Fire the 'after' trigger for an insert on this table
			connection.FireTableEvent(new TableModificationEvent(tableName, row, false));

			// NOTE: currently nothing being done with 'row_number' after it's added.
			//   The underlying table data source manages the row index.

		}

		/// <summary>
		/// Removes the given row from the table.
		/// </summary>
		/// <param name="rowNumber">The index of the row to delete.</param>
		/// <remarks>
		/// This is called just before the row is actually deleted. The method 
		/// is provided to allow for some maintenance of any search structures 
		/// such as B-Trees.
		/// This is called from the <see cref="Delete(Deveel.Data.Table)"/>.
		/// </remarks>
		private void RemoveRow(int rowNumber) {
			// This table name (for event notification)
			TableName tableName = TableName;

			// Fire the 'before' trigger for the delete on this table
			connection.FireTableEvent(new TableModificationEvent(tableName, rowNumber, true));

			// Delete the row from the underlying database
			dataSource.RemoveRow(rowNumber);

			// Fire the 'after' trigger for the delete on this table
			connection.FireTableEvent(new TableModificationEvent(tableName, rowNumber, false));
		}

		/// <summary>
		/// Updates the given row with the given data in this table.
		/// </summary>
		/// <param name="rowNumber"></param>
		/// <param name="row"></param>
		/// <remarks>
		/// This method will likely add the modified data to a new row and 
		/// delete the old version of the row.
		/// </remarks>
		private void UpdateRow(int rowNumber, DataRow row) {
			// This table name (for event notification)
			TableName tableName = TableName;

			// Fire the 'before' trigger for the update on this table
			connection.FireTableEvent(new TableModificationEvent(tableName, rowNumber, row, true));

			// Update the row in the underlying database
			dataSource.UpdateRow(rowNumber, row);

			// Fire the 'after' trigger for the update on this table
			connection.FireTableEvent(new TableModificationEvent(tableName, rowNumber, row, false));
		}

		/// <summary>
		/// Checks the database is in exclusive mode.
		/// </summary>
		private void CheckInExclusiveMode() {
			if (!IsInExclusiveMode)
				Debug.WriteException(new Exception("Performed exclusive operation on table and not in exclusive mode!"));
		}

		/// <summary>
		/// Check that we can safely Read from this table.
		/// </summary>
		private void CheckReadLock() {
#if DEBUG
			// All 'system' tables are given Read access because they may only be
			// written under exclusive mode anyway.

			bool isInternalTable = TableName.Schema.Equals(Database.SystemSchema);

			if (!(isInternalTable ||
			      debugReadLockCount > 0 ||
			      debugWriteLockCount > 0 ||
			      IsInExclusiveMode)) {

				Console.Error.WriteLine();
				Console.Error.Write(" isInternalTable = " + isInternalTable);
				Console.Error.Write(" debugReadLockCount = " + debugReadLockCount);
				Console.Error.Write(" debugWriteLockCount = " + debugWriteLockCount);
				Console.Error.WriteLine(" isInExclusiveMode = " + IsInExclusiveMode);

				Debug.WriteException(new ApplicationException("Invalid Read access on table '" + TableName + "'"));
			}
#endif
		}

		/// <summary>
		/// Check that we can safely read/write from this table.
		/// </summary>
		/// <remarks>
		/// This should catch any synchronization concurrent issues.
		/// </remarks>
		private void CheckReadWriteLock() {
#if DEBUG
			// We have to own exactly one Write Lock, or be in exclusive mode.
			if (!(debugWriteLockCount == 1 || IsInExclusiveMode))
				Debug.WriteException(new ApplicationException("Invalid Read/Write access on table '" + TableName + "'"));
#endif
		}

		/// <summary>
		/// Check that we can run a safe operation.
		/// </summary>
		private void CheckSafeOperation() {
			// no operation - nothing to check for...
		}

		/// <inheritdoc/>
		protected override void BlankSelectableSchemes(int type) {
		}

		/// <inheritdoc/>
		protected override SelectableScheme GetRootColumnScheme(int column) {
			CheckReadLock();  // Read op

			return dataSource.GetColumnScheme(column);
		}

		// -------- Methods implemented for DefaultDataTable --------

		/// <inheritdoc/>
		internal override void SetToRowTableDomain(int column, IntegerVector rowSet, ITableDataSource ancestor) {
			CheckReadLock();  // Read op

			if (ancestor != this && ancestor != dataSource)
				throw new Exception("Method routed to incorrect table ancestor.");
		}

		/// <summary>
		/// This is called by the <see cref="Lock"/> lass to notify this 
		/// <see cref="DataTable"/> that a read/write lock has been applied 
		/// to this table.
		/// </summary>
		/// <param name="lockType"></param>
		/// <remarks>
		/// This is for Lock debugging purposes only.
		/// </remarks>
		internal void OnReadWriteLockEstablish(AccessType lockType) {
#if DEBUG
			if (lockType == AccessType.Read) {
				++debugReadLockCount;
			} else if (lockType == AccessType.Write) {
				++debugWriteLockCount;
				if (debugWriteLockCount > 1)
					throw new ApplicationException(">1 write Lock on table " + TableName);
			} else {
				throw new ApplicationException("Unknown Lock type: " + lockType);
			}
#endif
		}

		/// <summary>
		/// This is called by <see cref="Lock"/> to notify this table that a 
		/// read/write lock has been released from it.
		/// </summary>
		/// <param name="lockType">The type of the lock released.</param>
		/// <remarks>
		/// This is for lock debugging purposes only.
		/// </remarks>
		internal void OnReadWriteLockRelease(AccessType lockType) {
#if DEBUG
			if (lockType == AccessType.Read) {
				--debugReadLockCount;
			} else if (lockType == AccessType.Write) {
				--debugWriteLockCount;
			} else {
				Debug.WriteException(new Exception("Unknown Lock type: " + lockType));
			}
#endif
		}

		/// <inheritdoc/>
		internal override SelectableScheme GetSelectableSchemeFor(int column, int originalColumn, Table table) {
			CheckReadLock();  // Read op
			return base.GetSelectableSchemeFor(column, originalColumn, table);
		}

		/// <inheritdoc/>
		internal override RawTableInformation ResolveToRawTable(RawTableInformation info) {
			CheckReadLock();  // Read op
			return base.ResolveToRawTable(info);
		}

		/// <summary>
		/// Declares the table as a new type.
		/// </summary>
		/// <param name="newName">The name of the declared table.</param>
		/// <returns>
		/// Returns a <see cref="ReferenceTable"/> representing the new 
		/// declaration of the table.
		/// </returns>
		public ReferenceTable DeclareAs(TableName newName) {
			return new ReferenceTable(this, newName);
		}

		/// <summary>
		/// Generates a new row for the addition of data to the table.
		/// </summary>
		/// <remarks>
		/// To add the data setted to the resultant <see cref="DataRow"/>
		/// object it must be passed to the <see cref="AddRow"/> method.
		/// </remarks>
		/// <returns>
		/// Returns a <see cref="DataRow"/> representing a row for the addition 
		/// of data to the table.
		/// </returns>
		public DataRow NewRow() {
			CheckSafeOperation();  // safe op
			return new DataRow(this);
		}

		///<summary>
		/// Adds a given <see cref="DataRow"/> object to the table.
		///</summary>
		///<param name="dataRow"></param>
		/// <remarks>
		/// This should be used for any rows added to the table. The order that rows are 
		/// added into a table is not important.
		/// <para>
		/// This method performs some checking of the cells in the table. It first checks 
		/// that all columns declared as <i>not null</i> have a value that is not* null. It 
		/// then checks that a the added row will not cause any duplicates in a column declared 
		/// as unique.
		/// </para>
		/// <para>
		/// It then uses the low level io manager to store the data.
		/// </para>
		/// <para>
		/// <b>Synchronization Issue</b>: We are assuming this is running in a synchronized environment 
		/// that is unable to add or alter rows in this object within the lifetime of this method.
		/// </para>
		/// </remarks>
		///<exception cref="DatabaseException"></exception>
		public void Add(DataRow dataRow) {
			CheckReadWriteLock();  // Write op

			if (!dataRow.IsSameTable(this))
				throw new DatabaseException("Internal Error: Using DataRow from different table");

			// Checks passed, so add to table.
			AddRow(dataRow);

			// Perform a referential integrity check on any changes to the table.
			dataSource.ConstraintIntegrityCheck();
		}

		/// <summary>
		/// Adds an array of new rows to the table.
		/// </summary>
		/// <param name="dataRowArr">The array of rows to add.</param>
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
		/// If the any of the given <paramref name="dataRowArr"/> was generated by 
		/// another data table.
		/// </exception>
		public void Add(IEnumerable<DataRow> dataRowArr) {
			CheckReadWriteLock();  // Write op

			foreach (DataRow dataRow in dataRowArr) {
				if (!dataRow.IsSameTable(this))
					throw new DatabaseException("Internal Error: Using DataRow from different table");

				AddRow(dataRow);
			}

			// Perform a referential integrity check on any changes to the table.
			dataSource.ConstraintIntegrityCheck();
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

			IntegerVector rowSet = new IntegerVector(table.RowCount);
			IRowEnumerator e = table.GetRowEnumerator();
			while (e.MoveNext()) {
				rowSet.AddInt(e.RowIndex);
			}

			// HACKY: Find the first column of this table in the search table.  This
			//   will allow us to generate a row set of only the rows in the search
			//   table.
			int firstColumn = table.FindFieldName(GetResolvedVariable(0));

			if (firstColumn == -1)
				throw new DatabaseException("Search table does not contain any " +
				                            "reference to table being deleted from");

			// Generate a row set that is in this tables domain.
			table.SetToRowTableDomain(firstColumn, rowSet, this);

			// row_set may contain duplicate row indices, therefore we must sort so
			// any duplicates are grouped and therefore easier to find.
			rowSet.QuickSort();

			// If limit less than zero then limit is whole set.
			if (limit < 0)
				limit = Int32.MaxValue;

			// Remove each row in row set in turn.  Make sure we don't remove the
			// same row index twice.
			int len = SysMath.Min(rowSet.Count, limit);
			int lastRemoved = -1;
			int removeCount = 0;
			for (int i = 0; i < len; ++i) {
				int toRemove = rowSet[i];
				if (toRemove < lastRemoved)
					throw new DatabaseException("Internal error: row sorting error or row_set not in the range > 0");

				if (toRemove != lastRemoved) {
					RemoveRow(toRemove);
					lastRemoved = toRemove;
					++removeCount;
				}

			}

			if (removeCount > 0)
				// Perform a referential integrity check on any changes to the table.
				dataSource.ConstraintIntegrityCheck();

			return removeCount;
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
		/// Deletes a single row that is at the current offset
		/// of the given cursor.
		/// </summary>
		/// <param name="cursor">The cursor used to locate the offset of
		/// the row to delete.</param>
		/// <returns>
		/// Returns 1 that indicates a single row was deleted.
		/// </returns>
		/// <exception cref="ArgumentException">
		/// If the given cursor refers to a table which is different from
		/// this one or if the given <paramref name="cursor"/> state is not
		/// <see cref="CursorState.Opened"/>.
		/// </exception>
		public int DeleteCurrent(Cursor cursor) {
			if (cursor.SelectedTable != this)
				throw new ArgumentException("Cannot delete from a cursor which is not on the table.");

			// it is possible to delete only from cursors marked
			// FOR UPDATE
			if (!cursor.IsUpdate)
				throw new InvalidOperationException("Cannot delete using a cursor not marked FOR UPDATE.");

			// it is not possible to determine a valid position of a
			// closed cursor
			if (cursor.State != CursorState.Opened)
				throw new ArgumentException("The cursor is not opened.");

			int rowIndex = cursor.RowIndex;
			if (rowIndex < 0)
				throw new ArgumentException("The cursor is not at a valid offset.");

			RemoveRow(rowIndex);

			// Perform a referential integrity check on any changes to the table.
			dataSource.ConstraintIntegrityCheck();

			return 1;
		}

		/// <summary>
		/// Updates the table by applying the assignment operations over each row
		/// that is found in the input table set.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="table"></param>
		/// <param name="assignList"></param>
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
		public int Update(IQueryContext context, Table table, Assignment[] assignList, int limit) {
			CheckReadWriteLock();  // Write op

			// Get the rows from the input table.
			IntegerVector rowSet = new IntegerVector();
			IRowEnumerator e = table.GetRowEnumerator();
			while (e.MoveNext()) {
				rowSet.AddInt(e.RowIndex);
			}

			// HACKY: Find the first column of this table in the search table.  This
			//   will allow us to generate a row set of only the rows in the search
			//   table.
			int firstColumn = table.FindFieldName(GetResolvedVariable(0));
			if (firstColumn == -1)
				throw new DatabaseException("Search table does not contain any " +
				                            "reference to table being updated from");

			// Convert the row_set to this table's domain.
			table.SetToRowTableDomain(firstColumn, rowSet, this);

			// NOTE: Assume there's no duplicate rows.

			DataRow originalData = NewRow();
			DataRow dataRow = NewRow();

			// If limit less than zero then limit is whole set.
			if (limit < 0)
				limit = Int32.MaxValue;

			// Update each row in row set in turn up to the limit.
			int len = SysMath.Min(rowSet.Count, limit);
			int updateCount = 0;
			for (int i = 0; i < len; ++i) {
				int toUpdate = rowSet[i];

				// Make a DataRow object from this row (plus keep the original intact
				// incase we need to roll back to it).
				originalData.SetFromRow(toUpdate);
				dataRow.SetFromRow(toUpdate);

				// Run each assignment on the DataRow.
				for (int n = 0; n < assignList.Length; ++n) {
					Assignment assignment = assignList[n];
					dataRow.Evaluate(assignment, context);
				}

				// Update the row
				UpdateRow(toUpdate, dataRow);

				++updateCount;
			}

			if (updateCount > 0)
				// Perform a referential integrity check on any changes to the table.
				dataSource.ConstraintIntegrityCheck();

			return updateCount;
		}

		/// <summary>
		/// </summary>
		/// <param name="context"></param>
		/// <param name="cursor"></param>
		/// <param name="assignList"></param>
		/// <returns></returns>
		public int UpdateCurrent(IQueryContext context, Cursor cursor, Assignment[] assignList) {
			CheckReadWriteLock();

			if (!cursor.IsUpdate)
				throw new InvalidOperationException("Cannot update a table from a cursor not marked FOR UPDATE.");

			if (cursor.SelectedTable != this)
				throw new ArgumentException("The cursor given evaluates to another table.");

			// NOTE: Assume there's no duplicate rows.

			DataRow originalData = NewRow();
			DataRow dataRow = NewRow();

			int rowIndex = cursor.RowIndex;

			// Make a DataRow object from this row (plus keep the original intact
			// incase we need to roll back to it).
			originalData.SetFromRow(rowIndex);
			dataRow.SetFromRow(rowIndex);

			for (int n = 0; n < assignList.Length; ++n) {
				Assignment assignment = assignList[n];
				dataRow.Evaluate(assignment, context);
			}

			// Update the row
			UpdateRow(rowIndex, dataRow);

			// Perform a referential integrity check on any changes to the table.
			dataSource.ConstraintIntegrityCheck();

			return 1;
		}

		/// <inheritdoc/>
		public override TObject GetCellContents(int column, int row) {
			CheckSafeOperation();  // safe op

			return dataSource.GetCellContents(column, row);
		}

		/// <inheritdoc/>
		public override IRowEnumerator GetRowEnumerator() {
			CheckReadLock(); // Read op

			return dataSource.GetRowEnumerator();
		}


		/// <inheritdoc/>
		public override void LockRoot(int lockKey) {
			CheckSafeOperation();  // safe op

			dataSource.AddRootLock();
		}

		/// <inheritdoc/>
		public override void UnlockRoot(int lockKey) {
			CheckSafeOperation();  // safe op

			dataSource.RemoveRootLock();
		}


		// ------------ Lock debugging methods ----------

		/// <summary>
		/// Returns true if the database is in exclusive mode.
		/// </summary>
		private bool IsInExclusiveMode {
			get {
				// Check the connection locking mechanism is in exclusive mode
				return connection.LockingMechanism.IsInExclusiveMode;
			}
		}

		/// <inheritdoc/>
		public override VariableName GetResolvedVariable(int column) {
			CheckSafeOperation();  // safe op

			return base.GetResolvedVariable(column);
		}

		/// <inheritdoc/>
		public override int FindFieldName(VariableName v) {
			CheckSafeOperation();  // safe op

			return base.FindFieldName(v);
		}
	}
}