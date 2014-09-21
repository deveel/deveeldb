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

using System;
using System.Collections.Generic;
using System.Data;

using Deveel.Data.Deveel.Diagnostics;
using Deveel.Data.Index;
using Deveel.Data.DbSystem;
using Deveel.Diagnostics;

namespace Deveel.Data.Transactions {
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
	internal partial class Transaction : SimpleTransaction, ICommitableTransaction {

		// ---------- Member variables ----------

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
		/// True if this transaction is closed.
		/// </summary>
		private bool closed;


		internal Transaction(TableDataConglomerate conglomerate, long commitId, IList<MasterTableDataSource> visibleTables, IList<IIndexSet> tableIndices)
			: base(conglomerate, conglomerate.SequenceManager) {

			Conglomerate = conglomerate;
			CommitId = commitId;
			closed = false;

			createdDatabaseObjects = new List<TableName>();
			droppedDatabaseObjects = new List<TableName>();

			touchedTables = new List<IMutableTableDataSource>();
			selectedFromTables = new List<MasterTableDataSource>();
			Journal = new TransactionJournal();

			// Set up all the visible tables
			int sz = visibleTables.Count;
			for (int i = 0; i < sz; ++i) {
				AddVisibleTable(visibleTables[i], tableIndices[i]);
			}

			// NOTE: We currently only support 8 - internal tables to the transaction
			//  layer, and internal tables to the database connection layer.
			internalTables = new IInternalTableContainer[8];
			internalTablesIndex = 0;
			AddInternalTableContainer(new TransactionInternalTables(this));

			Context.SystemContext.Stats.Increment("Transaction.count");

			// Defaults to true (should be changed by called 'setErrorOnDirtySelect'
			// method.
			TransactionErrorOnDirtySelect = true;
		}

		/// <summary>
		/// Returns the TableDataConglomerate of this transaction.
		/// </summary>
		internal TableDataConglomerate Conglomerate { get; private set; }

		/// <summary>
		/// Returns the 'commitId' which is the last commit that occured 
		/// before this transaction was created.
		/// </summary>
		/// <remarks>
		/// <b>Note</b> Don't make this synchronized over anything. This is 
		/// accessed by <see cref="OpenTransactionList"/>.
		/// </remarks>
		internal long CommitId { get; private set; }

		/// <summary>
		/// Gets the journal of changes made during this transaction.
		/// </summary>
		public TransactionJournal Journal { get; private set; }

		// ----- Operations within the context of this transaction -----

		/// <inheritdoc/>
		internal override IMutableTableDataSource CreateMutableTableDataSourceAtCommit(MasterTableDataSource master) {
			// Create the table for this transaction.
			IMutableTableDataSource table = master.CreateTableDataSourceAtCommit(this);
			// Log in the journal that this table was touched by the transaction.
			Journal.TouchedTable(master.TableId);
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
			var copyList = new List<MasterTableDataSource>(sz);

			MasterTableDataSource lastEntry = null;
			for (int i = 0; i < sz; ++i) {
				MasterTableDataSource masterTable = GetVisibleTable(i);
				TableName tableName = masterTable.TableInfo.TableName;
				if (tableName.Equals(SystemSchema.SysSequenceInfo)) {
					lastEntry = masterTable;
				} else {
					copyList.Add(masterTable);
				}
			}

			copyList.Insert(0, lastEntry);

			try {
				// For each master table,
				for (int i = 0; i < sz; ++i) {
					MasterTableDataSource masterTable = copyList[i];
					TableName tableName = masterTable.TableInfo.TableName;

					// Create a destination transaction
					Transaction destTransaction = destConglomerate.CreateTransaction();

					// The view of this table within this transaction.
					IIndexSet indexSet = GetIndexSetForTable(masterTable);

					// If the table already exists then drop it
					if (destTransaction.TableExists(tableName))
						destTransaction.DropTable(tableName);

					// Copy it into the destination conglomerate.
					destTransaction.CopyTable(masterTable, indexSet);

					// Close and commit the transaction in the destination conglomeration.      
					destTransaction.Commit();

					// Dispose the IIndexSet
					indexSet.Dispose();
				}
			} catch (TransactionException e) {
				Logger.Error(this, e);
				throw new Exception("Transaction Error when copying table: " + e.Message, e);
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
		/// conformance to <see cref="IsolationLevel.Serializable"/>
		/// isolation level this should return true.
		/// <para>
		/// <b>Note</b> We <b>must not</b> make this method serialized because 
		/// it is back called from within a commit lock in TableDataConglomerate.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		internal bool TransactionErrorOnDirtySelect { get; set; }

		/// <summary>
		/// Notifies this transaction that a database object with the given 
		/// name has successfully been created.
		/// </summary>
		/// <param name="tableName"></param>
		public void OnDatabaseObjectCreated(TableName tableName) {
			// If this table name was dropped, then remove from the drop list
			bool dropped = droppedDatabaseObjects.Contains(tableName);
			droppedDatabaseObjects.Remove(tableName);
			// If the above operation didn't remove a table name then add to the
			// created database objects list.
			if (!dropped) {
				createdDatabaseObjects.Add(tableName);
			}
		}

		/// <summary>
		/// Notifies this transaction that a database object with the given 
		/// name has successfully been dropped.
		/// </summary>
		/// <param name="tableName"></param>
		public void OnDatabaseObjectDropped(TableName tableName) {
			// If this table name was created, then remove from the create list
			bool created = createdDatabaseObjects.Contains(tableName);
			createdDatabaseObjects.Remove(tableName);
			// If the above operation didn't remove a table name then add to the
			// dropped database objects list.
			if (!created) {
				droppedDatabaseObjects.Add(tableName);
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
		/// Verifies whether a sequence generator for the given
		/// name exists.
		/// </summary>
		/// <param name="name"></param>
		/// <returns></returns>
		public bool  SequenceGeneratorExists(TableName name) {
			return SequenceManager.SequenceGeneratorExists(this, name);
		}

		/// <summary>
		/// Creates a new sequence generator with the given TableName and 
		/// initializes it with the given details.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="startValue"></param>
		/// <param name="incrementBy"></param>
		/// <param name="minValue"></param>
		/// <param name="maxValue"></param>
		/// <param name="cache"></param>
		/// <param name="cycle"></param>
		/// <remarks>
		/// This does <b>not</b> check if the given name clashes with an existing 
		/// database object.
		/// </remarks>
		public void CreateSequenceGenerator(TableName name, long startValue, long incrementBy, long minValue, long maxValue, long cache, bool cycle) {
			SequenceManager.CreateSequenceGenerator(this, name, startValue, incrementBy, minValue, maxValue, cache, cycle);

			// Notify that this database object has been created
			OnDatabaseObjectCreated(name);
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
					Conglomerate.ProcessCommit(this, VisibleTables, selectedFromTables, touchedTables, Journal);
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
					Conglomerate.ProcessRollback(this, touchedTables, Journal);
				} finally {
					Cleanup();
				}
			}

		}

		/// <summary>
		/// Cleans up this transaction.
		/// </summary>
		private void Cleanup() {
			Context.SystemContext.Stats.Decrement(StatsDefaultKeys.TransactionsCount);

			// Dispose of all the IIndexSet objects created by this transaction.
			DisposeAllIndices();

			// Dispose all the table we touched
			try {
				foreach (IMutableTableDataSource source in touchedTables) {
					source.Dispose();
				}
			} catch (Exception e) {
				Logger.Error(this, e);
			}

			Context.SystemContext.Stats.Increment("Transaction.Cleanup");
			Conglomerate = null;
			touchedTables = null;
			Journal = null;

			// Dispose all the cursors in the transaction
			ClearCursors();

			Variables.Clear();
		}

		/// <summary>
		/// Closes this transaction without rolling back or committing the changes.
		/// </summary>
		/// <remarks>
		/// Care should be taken when using this - it must only be used for simple
		/// transactions that are short lived and have not modified the database.
		/// </remarks>
		internal void CloseAndCleanup() {
			if (!IsReadOnly)
				throw new Exception("Assertion failed - tried to dispose a non Read-only transaction.");

			if (!closed) {
				closed = true;
				Cleanup();
			}
		}

		// ---------- Transaction inner classes ----------

		/// <summary>
		/// A list of DataTableInfo system table definitions for tables internal 
		/// to the transaction.
		/// </summary>
		private readonly static DataTableInfo[] InternalInfoList;

		static Transaction() {
			InternalInfoList = new DataTableInfo[4];
			InternalInfoList[0] = SystemSchema.TableColumnsTableInfo;
			InternalInfoList[1] = SystemSchema.TableInfoTableInfo;
			InternalInfoList[2] = SystemSchema.ProductInfoTableInfo;
			InternalInfoList[3] = SystemSchema.VariablesTableInfo;
		}

		/// <summary>
		/// A static internal table info for internal tables to the transaction.
		/// </summary>
		/// <remarks>
		/// This implementation includes all the dynamically generated system tables
		/// that are tied to information in a transaction.
		/// </remarks>
		private class TransactionInternalTables : InternalTableContainer {
			private readonly Transaction transaction;

			public TransactionInternalTables(Transaction transaction)
				: base("SYSTEM TABLE", InternalInfoList) {
				this.transaction = transaction;
			}

			// ---------- Implemented ----------

			public override ITableDataSource CreateInternalTable(int index) {
				if (index == 0)
					return SystemSchema.GetTableColumnsTable(transaction);
				if (index == 1)
					return SystemSchema.GetTableInfoTable(transaction);
				if (index == 2)
					return SystemSchema.GetProductInfoTable(transaction);
				if (index == 3)
					return SystemSchema.GetVariablesTable(transaction);
				
				throw new Exception();
			}

		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (!closed) {
					Logger.Error(this, "Transaction not closed!");
					Rollback();
				}				
			}

			base.Dispose(disposing);
		}
	}
}