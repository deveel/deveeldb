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
using System.Collections;
using System.Collections.Generic;

using Deveel.Data.Collections;
using Deveel.Data.QueryPlanning;
using Deveel.Diagnostics;

namespace Deveel.Data {
	/// <summary>
	/// An open transaction that manages all data access to the <see cref="TableDataConglomerate"/>.
	/// </summary>
	/// <remarks>
	/// A transaction sees a view of the data as it was when the transaction 
	/// was created.  It also sees any modifications that were made within the 
	/// context of this transaction.  It does not see modifications made
	/// by other open transactions.
	/// <para>
	/// A transaction ends when it is committed or rollbacked. All operations
	/// on this transaction object only occur within the context of this 
	/// transaction and are not permanent changes to the database structure. 
	/// Only when the transaction is committed are changes reflected in the 
	/// master data.
	/// </para>
	/// </remarks>
	public partial class Transaction : SimpleTransaction, IDisposable, ICursorContext {

		// ---------- Member variables ----------

		/// <summary>
		/// The TableDataConglomerate that this transaction is within the context of.
		/// </summary>
		private TableDataConglomerate conglomerate;

		/// <summary>
		/// The commitId that represents the id of the last commit that occurred
		/// when this transaction was created.
		/// </summary>
		private readonly long commitId;

		/// <summary>
		/// The name of all database objects that were created in this transaction.
		/// This is used for a namespace collision test during commit.
		/// </summary>
		private readonly IList<TableName> createdDatabaseObjects;

		/// <summary>
		/// The name of all database objects that were dropped in this transaction.
		/// This is used for a namespace collision test during commit.
		/// </summary>
		private readonly IList<TableName> droppedDatabaseObjects;

		/// <summary>
		/// The list of temporary tables, which survive for the time of the
		/// transaction: when committed or rolledback they will be disposed.
		/// </summary>
		private readonly ArrayList temporary_tables;

		private Hashtable cursors;

		/// <summary>
		/// The journal for this transaction.  This journal describes all changes
		/// made to the database by this transaction.
		/// </summary>
		private TransactionJournal journal;

		/// <summary>
		/// True if an error should be generated on a dirty select.
		/// </summary>
		private bool transaction_error_on_dirty_select;

		/// <summary>
		/// True if this transaction is closed.
		/// </summary>
		private bool closed;


		internal Transaction(TableDataConglomerate conglomerate, long commitId, IList<MasterTableDataSource> visibleTables, IList<IIndexSet> tableIndices)
			: base(conglomerate.System, conglomerate.SequenceManager) {

			this.conglomerate = conglomerate;
			this.commitId = commitId;
			closed = false;

			createdDatabaseObjects = new List<TableName>();
			droppedDatabaseObjects = new List<TableName>();

			touchedTables = new List<IMutableTableDataSource>();
			selectedFromTables = new List<MasterTableDataSource>();
			temporary_tables = new ArrayList();
			journal = new TransactionJournal();

			cursors = new Hashtable();

			// Set up all the visible tables
			int sz = visibleTables.Count;
			for (int i = 0; i < sz; ++i) {
				AddVisibleTable(visibleTables[i], tableIndices[i]);
			}

			// NOTE: We currently only support 8 - internal tables to the transaction
			//  layer, and internal tables to the database connection layer.
			internalTables = new IInternalTableInfo[8];
			internalTablesIndex = 0;
			AddInternalTableInfo(new TransactionInternalTables(this));

			System.Stats.Increment("Transaction.count");

			// Defaults to true (should be changed by called 'setErrorOnDirtySelect'
			// method.
			transaction_error_on_dirty_select = true;
		}

		/// <summary>
		/// Returns the TableDataConglomerate of this transaction.
		/// </summary>
		internal TableDataConglomerate Conglomerate {
			get { return conglomerate; }
		}

		/// <summary>
		/// Returns the 'commitId' which is the last commit that occured 
		/// before this transaction was created.
		/// </summary>
		/// <remarks>
		/// <b>Note</b> Don't make this synchronized over anything. This is 
		/// accessed by <see cref="OpenTransactionList"/>.
		/// </remarks>
		internal long CommitID {
			get {
				// REINFORCED NOTE: This absolutely must never be synchronized because
				//   it is accessed by OpenTransactionList synchronized.
				return commitId;
			}
		}

		// ----- Operations within the context of this transaction -----

		/// <inheritdoc/>
		internal override IMutableTableDataSource CreateMutableTableDataSourceAtCommit(MasterTableDataSource master) {
			// Create the table for this transaction.
			IMutableTableDataSource table = master.CreateTableDataSourceAtCommit(this);
			// Log in the journal that this table was touched by the transaction.
			journal.EntryAddTouchedTable(master.TableID);
			touchedTables.Add(table);
			return table;
		}

		/// <summary>
		/// Copies all the tables within this transaction view to the 
		/// destination conglomerate object.
		/// </summary>
		/// <param name="destConglomerate"></param>
		/// <remarks>
		/// Some care should be taken with security when using this method. 
		/// This is useful for generating a backup of the current view of the 
		/// database that can work without interfering with the general
		/// operation of the database.
		/// </remarks>
		internal void LiveCopyAllDataTo(TableDataConglomerate destConglomerate) {
			// Create a new TableDataConglomerate using the same settings from this
			// TransactionSystem but on the new IStoreSystem.
			int sz = VisibleTableCount;

			// The list to copy (in the order to copy in).
			// We WriteByte the 'SEQUENCE_INFO' at the very end of the table list to copy.
			ArrayList copy_list = new ArrayList(sz);

			MasterTableDataSource lastEntry = null;
			for (int i = 0; i < sz; ++i) {
				MasterTableDataSource master_table = GetVisibleTable(i);
				TableName table_name = master_table.DataTableInfo.TableName;
				if (table_name.Equals(TableDataConglomerate.SysSequenceInfo)) {
					lastEntry = master_table;
				} else {
					copy_list.Add(master_table);
				}
			}
			copy_list.Insert(0, lastEntry);

			try {
				// For each master table,
				for (int i = 0; i < sz; ++i) {

					MasterTableDataSource master_table =
										   (MasterTableDataSource)copy_list[i];
					TableName table_name = master_table.DataTableInfo.TableName;

					// Create a destination transaction
					Transaction dest_transaction = destConglomerate.CreateTransaction();

					// The view of this table within this transaction.
					IIndexSet index_set = GetIndexSetForTable(master_table);

					// If the table already exists then drop it
					if (dest_transaction.TableExists(table_name)) {
						dest_transaction.DropTable(table_name);
					}

					// Copy it into the destination conglomerate.
					dest_transaction.CopyTable(master_table, index_set);

					// Close and commit the transaction in the destination conglomeration.      
					dest_transaction.Commit();

					// Dispose the IIndexSet
					index_set.Dispose();

				}

			} catch (TransactionException e) {
				Debug.WriteException(e);
				throw new Exception("Transaction Error when copying table: " +
										   e.Message);
			}
		}


		// ---------- Transaction manipulation ----------

		/// <summary>
		/// Gets or sets if the conglomerate commit procedure should check for
		/// dirty selects and produce a transaction error.
		/// </summary>
		/// <remarks>
		/// A dirty select is when a query reads information from a table 
		/// that is effected by another table during a transaction. This in 
		/// itself will not cause data consistancy problems but for strict 
		/// conformance to <see cref="System.Data.IsolationLevel.Serializable"/>
		/// isolation level this should return true.
		/// <para>
		/// <b>Note</b> We <b>must not</b> make this method serialized because 
		/// it is back called from within a commit lock in TableDataConglomerate.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		internal bool TransactionErrorOnDirtySelect {
			get { return transaction_error_on_dirty_select; }
			set { transaction_error_on_dirty_select = value; }
		}


		/// <summary>
		/// Convenience, given a <see cref="SimpleTableQuery"/> object this 
		/// will return a list of column names in sequence that represent the 
		/// columns in a group constraint.
		/// </summary>
		/// <param name="dt"></param>
		/// <param name="cols">The unsorted list of indexes in the table that 
		/// represent the group.</param>
		/// <remarks>
		/// Assumes column 2 of dt is the sequence number and column 1 is the name
		/// of the column.
		/// </remarks>
		/// <returns></returns>
		private static String[] ToColumns(SimpleTableQuery dt, IntegerVector cols) {
			int size = cols.Count;
			String[] list = new String[size];

			// for each n of the output list
			for (int n = 0; n < size; ++n) {
				// for each i of the input list
				for (int i = 0; i < size; ++i) {
					int row_index = cols[i];
					int seq_no = ((BigNumber)dt.Get(2, row_index).Object).ToInt32();
					if (seq_no == n) {
						list[n] = dt.Get(1, row_index).Object.ToString();
						break;
					}
				}
			}

			return list;
		}

		/// <summary>
		/// Notifies this transaction that a database object with the given 
		/// name has successfully been created.
		/// </summary>
		/// <param name="table_name"></param>
		internal void OnDatabaseObjectCreated(TableName table_name) {
			// If this table name was dropped, then remove from the drop list
			bool dropped = droppedDatabaseObjects.Contains(table_name);
			droppedDatabaseObjects.Remove(table_name);
			// If the above operation didn't remove a table name then add to the
			// created database objects list.
			if (!dropped) {
				createdDatabaseObjects.Add(table_name);
			}
		}

		/// <summary>
		/// Notifies this transaction that a database object with the given 
		/// name has successfully been dropped.
		/// </summary>
		/// <param name="table_name"></param>
		internal void OnDatabaseObjectDropped(TableName table_name) {
			// If this table name was created, then remove from the create list
			bool created = createdDatabaseObjects.Contains(table_name);
			createdDatabaseObjects.Remove(table_name);
			// If the above operation didn't remove a table name then add to the
			// dropped database objects list.
			if (!created) {
				droppedDatabaseObjects.Add(table_name);
			}
		}

		/// <summary>
		/// Returns the normalized list of database object names created 
		/// in this transaction.
		/// </summary>
		internal IList<TableName> AllNamesCreated {
			get { return createdDatabaseObjects; }
		}

		/// <summary>
		/// Returns the normalized list of database object names dropped 
		/// in this transaction.
		/// </summary>
		internal IList<TableName> AllNamesDropped {
			get { return droppedDatabaseObjects; }
		}

		/// <summary>
		/// Sets a persistent variable of the database that becomes a committed
		/// change once this transaction is committed.
		/// </summary>
		/// <param name="variable"></param>
		/// <param name="value"></param>
		/// <remarks>
		/// The variable can later be retrieved with a call to the 
		/// <see cref="GetPersistantVariable"/> method.  A persistant var is created 
		/// if it doesn't exist in the DatabaseVars table otherwise it is 
		/// overwritten.
		/// </remarks>
		public void SetPersistentVariable(String variable, String value) {
			TableName table_name = TableDataConglomerate.PersistentVarTable;
			IMutableTableDataSource t = GetTable(table_name);
			SimpleTableQuery dt = new SimpleTableQuery(t);
			dt.SetVariable(0, new Object[] { variable, value });
			dt.Dispose();
		}

		/// <summary>
		/// Returns the value of the persistent variable with the given name 
		/// or null if it doesn't exist.
		/// </summary>
		/// <param name="variable"></param>
		/// <returns></returns>
		public String GetPersistantVariable(String variable) {
			TableName table_name = TableDataConglomerate.PersistentVarTable;
			IMutableTableDataSource t = GetTable(table_name);
			SimpleTableQuery dt = new SimpleTableQuery(t);
			String val = dt.GetVariable(1, 0, variable).ToString();
			dt.Dispose();
			return val;
		}

		/// <summary>
		/// Creates a new sequence generator with the given TableName and 
		/// initializes it with the given details.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="start_value"></param>
		/// <param name="increment_by"></param>
		/// <param name="min_value"></param>
		/// <param name="max_value"></param>
		/// <param name="cache"></param>
		/// <param name="cycle"></param>
		/// <remarks>
		/// This does <b>not</b> check if the given name clashes with an existing 
		/// database object.
		/// </remarks>
		public void CreateSequenceGenerator(
					 TableName name, long start_value, long increment_by,
					 long min_value, long max_value, long cache, bool cycle) {
			SequenceManager.CreateSequenceGenerator(this,
				 name, start_value, increment_by, min_value, max_value, cache,
				 cycle);

			// Notify that this database object has been created
			OnDatabaseObjectCreated(name);
		}

		/// <summary>
		/// Verifies whether a sequence generator for the given
		/// name exists.
		/// </summary>
		/// <param name="name"></param>
		/// <returns></returns>
		public bool  SequenceGeneratorExists(TableName name) {
			return SequenceManager.SequenceGeneratorExists(this, name);
		}

		/// <summary>
		/// Drops an existing sequence generator with the given name.
		/// </summary>
		/// <param name="name"></param>
		public void DropSequenceGenerator(TableName name) {
			SequenceManager.DropSequenceGenerator(this, name);
			// Flush the sequence manager
			FlushSequenceManager(name);

			// Notify that this database object has been dropped
			OnDatabaseObjectDropped(name);
		}

		// ----- Transaction close operations -----

		/// <summary>
		/// Closes and marks a transaction as committed.
		/// </summary>
		/// <remarks>
		/// Any changes made by this transaction are seen by all transactions 
		/// created after this method returns.
		/// <para>
		/// This method will fail under the following circumstances:
		/// <list type="bullet">
		/// <item>There are any rows deleted in this transaction that were 
		///	deleted by another successfully committed transaction.</item>
		///	<item>There were rows added in another committed transaction 
		///	that would change the result of the search clauses committed by 
		///	this transaction.</item>
		/// </list>
		///	The first check is not too difficult to check for. The second is 
		///	very difficult however we need it to ensure 
		///	<see cref="System.Data.IsolationLevel.Serializable"/> isolation is 
		///	enforced. We may have to simplify this by throwing a transaction 
		///	exception if the table has had any changes made to it during this 
		///	transaction.
		/// </para>
		///	<para>
		///	This should only be called under an exclusive lock on the connection.
		///	</para>
		/// </remarks>
		public void Commit() {
			if (!closed) {
				try {
					closed = true;
					// Get the conglomerate to do this commit.
					conglomerate.ProcessCommit(this, VisibleTables,
											   selectedFromTables,
											   touchedTables, journal);
				} finally {
					Cleanup();
				}
			}

		}

		/// <summary>
		/// Closes and rolls back a transaction as if the commands the 
		/// transaction ran never happened.
		/// </summary>
		/// <remarks>
		/// This should only be called under an exclusive Lock on the connection.
		/// <para>
		/// This will not throw a transaction exception.
		/// </para>
		/// </remarks>
		public void Rollback() {
			if (!closed) {
				try {
					closed = true;
					// Notify the conglomerate that this transaction has closed.
					conglomerate.ProcessRollback(this, touchedTables, journal);
				} finally {
					Cleanup();
				}
			}

		}

		/// <summary>
		/// Cleans up this transaction.
		/// </summary>
		private void Cleanup() {
			System.Stats.Decrement("Transaction.count");
			// Dispose of all the IIndexSet objects created by this transaction.
			DisposeAllIndices();

			// Dispose all the table we touched
			try {
				for (int i = 0; i < touchedTables.Count; ++i) {
					IMutableTableDataSource source =
										   (IMutableTableDataSource)touchedTables[i];
					source.Dispose();
				}
			} catch (Exception e) {
				Debug.WriteException(e);
			}

			System.Stats.Increment("Transaction.Cleanup");
			conglomerate = null;
			touchedTables = null;
			journal = null;

			// Dispose all the cursors in the transaction
			ClearCursors();

			Variables.Clear();
		}

		/**
		 * Disposes this transaction without rolling back or committing the changes.
		 * Care should be taken when using this - it must only be used for simple
		 * transactions that are short lived and have not modified the database.
		 */

		internal void dispose() {
			if (!IsReadOnly) {
				throw new Exception(
					"Assertion failed - tried to dispose a non Read-only transaction.");
			}
			if (!closed) {
				closed = true;
				Cleanup();
			}
		}

		// ---------- Cursor management ----------

		void ICursorContext.OnCursorCreated(Cursor cursor) {
			cursors[cursor.Name] = cursor;

			OnDatabaseObjectCreated(cursor.Name);
		}

		void ICursorContext.OnCursorDisposing(Cursor cursor) {
			RemoveCursor(cursor.Name);
		}

		public Cursor DeclareCursor(TableName name, IQueryPlanNode queryPlan, CursorAttributes attributes) {
			if (cursors.ContainsKey(name))
				throw new ArgumentException("The cursor '" + name + "' was already defined within this transaction.");

			return new Cursor(this, name, queryPlan, attributes);
		}

		public Cursor DeclareCursor(TableName name, IQueryPlanNode queryPlan) {
			return DeclareCursor(name, queryPlan, CursorAttributes.ReadOnly);
		}

		public Cursor GetCursor(TableName name) {
			if (name == null)
				throw new ArgumentNullException("name");

			Cursor cursor = cursors[name] as Cursor;
			if (cursor == null)
				throw new ArgumentException("Cursor '" + name + "' was not declared.");

			if (cursor.State == CursorState.Broken)
				throw new InvalidOperationException("The state of the cursor is invalid.");

			return cursor;
		}

		public void RemoveCursor(TableName name) {
			if (name == null)
				throw new ArgumentNullException("name");

			Cursor cursor = cursors[name] as Cursor;
			if (cursor == null)
				throw new ArgumentException("Cursor '" + name + "' was not declared.");

			cursor.InternalDispose();
			cursors.Remove(name);

			OnDatabaseObjectDropped(name);
		}

		public bool CursorExists(TableName name) {
			return cursors.ContainsKey(name);
		}

		protected void ClearCursors() {
			ArrayList cursorsList = new ArrayList(cursors.Values);
			for (int i = cursorsList.Count - 1; i >= 0; i--) {
				Cursor cursor = cursorsList[i] as Cursor;
				if (cursor == null)
					continue;

				cursor.Dispose();
			}

			cursors.Clear();
			cursors = null;
		}

		// ----------- UDT Management --------------------

		public void CreateUserType(UserType userType) {
			TableName typeName = userType.Name;
			if (UserTypeExists(typeName))
				throw new InvalidOperationException("The type " + typeName + " already exists.");

			UDTManager.CreateType(this, userType);

			OnDatabaseObjectCreated(typeName);
		}

		public void DropUserType(TableName typeName) {
			if (!UserTypeExists(typeName))
				throw new InvalidOperationException("The type '" + typeName + "' was not found.");

			UDTManager.DropType(this, typeName);

			OnDatabaseObjectDropped(typeName);
		}

		public UserType GetUserType(TableName typeName) {
			return UDTManager.GetUserTypeDef(this, typeName);
		}

		public bool UserTypeExists(TableName typeName) {
			return UDTManager.TypeExists(this, typeName);
		}


		// ---------- Transaction inner classes ----------

		/// <summary>
		/// A list of DataTableInfo system table definitions for tables internal 
		/// to the transaction.
		/// </summary>
		private readonly static DataTableInfo[] InternalInfoList;

		static Transaction() {
			InternalInfoList = new DataTableInfo[4];
			InternalInfoList[0] = GTTableColumnsDataSource.InfoDataTableInfo;
			InternalInfoList[1] = GTTableInfoDataSource.InfoDataTableInfo;
			InternalInfoList[2] = GTProductDataSource.InfoDataTableInfo;
			InternalInfoList[3] = GTVariablesDataSource.InfoDataTableInfo;
		}

		/// <summary>
		/// A static internal table info for internal tables to the transaction.
		/// </summary>
		/// <remarks>
		/// This implementation includes all the dynamically generated system tables
		/// that are tied to information in a transaction.
		/// </remarks>
		private class TransactionInternalTables : InternalTableInfo {
			private readonly Transaction transaction;

			public TransactionInternalTables(Transaction transaction)
				: base("SYSTEM TABLE", InternalInfoList) {
				this.transaction = transaction;
			}

			// ---------- Implemented ----------

			public override IMutableTableDataSource CreateInternalTable(int index) {
				if (index == 0)
					return new GTTableColumnsDataSource(transaction).Init();
				if (index == 1)
					return new GTTableInfoDataSource(transaction).Init();
				if (index == 2)
					return new GTProductDataSource(transaction).Init();
				if (index == 3)
					return new GTVariablesDataSource(transaction).Init();
				
				throw new Exception();
			}

		}

		/// <summary>
		/// A group of columns in a table as used by the constraint system.
		/// </summary>
		public sealed class ColumnGroup {
			/// <summary>
			/// The name of the group (the constraint name).
			/// </summary>
			public String name;

			/// <summary>
			/// The list of columns that make up the group.
			/// </summary>
			public String[] columns;

			/// <summary>
			/// Whether this is deferred or initially immediate.
			/// </summary>
			public ConstraintDeferrability deferred;

		}

		/// <summary>
		/// Represents a constraint expression to check.
		/// </summary>
		public sealed class CheckExpression {
			/// <summary>
			/// The name of the check expression (the constraint name).
			/// </summary>
			public String name;

			/// <summary>
			/// The expression to check.
			/// </summary>
			public Expression expression;

			/// <summary>
			/// Whether this is deferred or initially immediate.
			/// </summary>
			public ConstraintDeferrability deferred;

		}

		/// <summary>
		/// Represents a reference from a group of columns in one table to a group of
		/// columns in another table.
		/// </summary>
		/// <remarks>
		/// This class is commonly used to represent a foreign key reference.
		/// </remarks>
		public sealed class ColumnGroupReference {

			///<summary>
			/// The name of the group (the constraint name).
			///</summary>
			public String name;

			///<summary>
			/// The key table name.
			///</summary>
			public TableName key_table_name;

			///<summary>
			/// The list of columns that make up the key.
			///</summary>
			public String[] key_columns;

			///<summary>
			/// The referenced table name.
			///</summary>
			public TableName ref_table_name;

			///<summary>
			/// The list of columns that make up the referenced group.
			///</summary>
			public String[] ref_columns;

			///<summary>
			/// The update rule.
			///</summary>
			public ConstraintAction update_rule;

			///<summary>
			/// The delete rule.
			///</summary>
			public ConstraintAction delete_rule;

			///<summary>
			/// Whether this is deferred or initially immediate.
			///</summary>
			public ConstraintDeferrability deferred;

		}

		#region Implementation of IDisposable

		void IDisposable.Dispose() {
			if (!closed) {
				Debug.Write(DebugLevel.Error, this, "Transaction not closed!");
				Rollback();
			}
		}

		#endregion
	}
}