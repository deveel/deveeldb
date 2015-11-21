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
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Deveel.Data.Index;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Store;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	class TableSourceComposite : ITableSourceComposite, IDisposable {
		private readonly object commitLock = new object();
		private Dictionary<int, TableSource> tableSources;

		private List<TransactionObjectState> objectStates; 

		private IStoreSystem tempStoreSystem;
		private IStore lobStore;
		private IStore stateStore;

		private const string StateStorePostfix = "_sf";

		public const string ObjectStoreName = "lob_store";

		public TableSourceComposite(Database database) {
			Database = database;

			tempStoreSystem = new InMemoryStorageSystem();
			objectStates = new List<TransactionObjectState>();

			StateStoreName = String.Format("{0}_{1}", database.Name(), StateStorePostfix);

			Setup();
		}

		~TableSourceComposite() {
			Dispose(false);
		}

		public Database Database { get; private set; }

		public IDatabaseContext DatabaseContext {
			get { return Database.DatabaseContext; }
		}

		private IStoreSystem StoreSystem {
			get { return DatabaseContext.StoreSystem; }
		}

		public int CurrentCommitId { get; private set; }

		private bool IsReadOnly {
			get { return Database.DatabaseContext.ReadOnly(); }
		}

		private bool IsClosed {
			get { return tableSources == null; }
		}

		private TableStateStore StateStore { get; set; }

		private string StateStoreName { get; set; }

		private IObjectStore LargeObjectStore { get; set; }

		private void ReadVisibleTables() {
			lock (commitLock) {
				var tables = StateStore.GetVisibleList();

				// For each visible table
				foreach (var resource in tables) {
					var tableId = resource.TableId;
					var sourceName = resource.SourceName;

					// TODO: add a table source type?

					// Load the master table from the resource information
					var source = LoadTableSource(tableId, sourceName);

					if (source == null)
						throw new InvalidOperationException(String.Format("Table {0} was not found.", sourceName));

					source.Open();

					tableSources.Add(tableId, source);
				}
			}
		}

		private void ReadDroppedTables() {
			lock (commitLock) {
				// The list of all dropped tables from the state file
				var tables = StateStore.GetDeleteList();

				// For each visible table
				foreach (var resource in tables) {
					int tableId =resource.TableId;
					string tableName = resource.SourceName;

					// Load the master table from the resource information
					var source = LoadTableSource(tableId, tableName);

					// File wasn't found so remove from the delete resources
					if (source == null) {
						StateStore.RemoveDeleteResource(tableName);
					} else {
						source.Open();

						// Add the table to the table list
						tableSources.Add(tableId, source);
					}
				}

				StateStore.Flush();
			}
		}

		private TableSource LoadTableSource(int tableId, string tableName) {
			var source = new TableSource(this, StoreSystem, LargeObjectStore, tableId, tableName);
			if (!source.Exists())
				return null;

			return source;
		}

		private void MarkUncommitted(int tableId) {
			var masterTable = GetTableSource(tableId);
			StateStore.AddDeleteResource(new TableStateStore.TableState(tableId, masterTable.SourceName));
		}

		//public ITransaction CreateTransaction(TransactionIsolation isolation) {
		//	var thisCommittedTables = new List<TableSource>();

		//	// Don't let a commit happen while we are looking at this.
		//	lock (commitLock) {
		//		long thisCommitId = CurrentCommitId;
		//		var committedTableList = StateStore.GetVisibleList();
		//		thisCommittedTables.AddRange(committedTableList.Select(resource => GetTableSource(resource.TableId)));

		//		// Create a set of IIndexSet for all the tables in this transaction.
		//		var indexInfo = (thisCommittedTables.Select(mtable => mtable.CreateIndexSet())).ToList();

		//		// Create the transaction and record it in the open transactions list.
		//		var t = new Transaction(this, thisCommitId, isolation, thisCommittedTables, indexInfo);
		//		openTransactions.AddTransaction(t);
		//		return t;
		//	}

		//}

		private void Dispose(bool disposing) {
			if (disposing) {
				Close();

				if (lobStore != null)
					lobStore.Dispose();
				if (stateStore != null)
					stateStore.Dispose();

				if (tempStoreSystem != null)
					tempStoreSystem.Dispose();

				tempStoreSystem = null;
				lobStore = null;
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Setup() {
			lock (this) {
				CurrentCommitId = 0;
				tableSources = new Dictionary<int, TableSource>();				
			}
		}

		private void InitObjectStore() {
			// Does the file already exist?
			bool blobStoreExists = StoreSystem.StoreExists(ObjectStoreName);
			// If the blob store doesn't exist and we are read_only, we can't do
			// anything further so simply return.
			if (!blobStoreExists && IsReadOnly) {
				return;
			}

			// The blob store,
			if (blobStoreExists) {
				lobStore = StoreSystem.OpenStore(ObjectStoreName);
			} else {
				lobStore = StoreSystem.CreateStore(ObjectStoreName);
			}

			try {
				lobStore.Lock();

				// TODO: have multiple BLOB stores
				LargeObjectStore = new ObjectStore(0, lobStore);

				// Get the 64 byte fixed area
				var fixedArea = lobStore.GetArea(-1, false);
				// If the blob store didn't exist then we need to create it here,
				if (!blobStoreExists) {
					long headerP = LargeObjectStore.Create();
					fixedArea.WriteInt8(headerP);
					fixedArea.Flush();
				} else {
					// Otherwise we need to initialize the blob store
					long headerP = fixedArea.ReadInt8();
					LargeObjectStore.Open(headerP);
				}
			} finally {
				lobStore.Unlock();
			}
		}

		private void CleanUp() {
			lock (commitLock) {
				if (IsClosed)
					return;

				// If no open transactions on the database, then clean up.
				if (Database.TransactionFactory.OpenTransactions.Count == 0) {
					var deleteList = StateStore.GetDeleteList().ToArray();
					if (deleteList.Length > 0) {
						int dropCount = 0;

						for (int i = deleteList.Length - 1; i >= 0; --i) {
							var tableName = deleteList[i].SourceName;
							CloseTable(tableName, true);
						}

						for (int i = deleteList.Length - 1; i >= 0; --i) {
							string tableName = deleteList[i].SourceName;
							bool dropped = CloseAndDropTable(tableName);
							// If we managed to drop the table, remove from the list.
							if (dropped) {
								StateStore.RemoveDeleteResource(tableName);
								++dropCount;
							}
						}

						// If we dropped a table, commit an update to the conglomerate state.
						if (dropCount > 0)
							StateStore.Flush();
					}
				}
			}
		}

		private bool CloseAndDropTable(string tableFileName) {
			// Find the table with this file name.
			int? tableId = null;
			foreach (var source in tableSources.Values) {
				if (source.StoreIdentity.Equals(tableFileName)) {
					if (source.IsRootLocked)
						return false;

					if (!source.Drop())
						return false;

					tableId = source.TableId;
				}
			}

			if (tableId != null)
				tableSources.Remove(tableId.Value);

			return false;
		}

		private void CloseTable(string sourceName, bool pendingDrop) {
			// Find the table with this file name.
			foreach (var source in tableSources.Values) {
				if (source.SourceName.Equals(sourceName)) {
					if (source.IsRootLocked)
						break;

					source.Close(pendingDrop);
					break;
				}
			}
		}

		public bool Exists() {
			return StoreSystem.StoreExists(StateStoreName);
		}

		public void Open() {
			if (!Exists())
				throw new IOException("Table composite does not exist");

			// Check the file Lock
			if (!IsReadOnly) {
				// Obtain the Lock (generate error if this is not possible)
				StoreSystem.Lock(StateStoreName);
			}

			// Open the state store
			stateStore = StoreSystem.OpenStore(StateStoreName);
			StateStore = new TableStateStore(stateStore);

			// Get the fixed 64 byte area.
			var fixedArea = stateStore.GetArea(-1);
			long headP = fixedArea.ReadInt8();
			StateStore.Open(headP);

			Setup();

			InitObjectStore();

			ReadVisibleTables();
			ReadDroppedTables();

			CleanUp();
		}

		public void Create() {
			MinimalCreate();

			// Initialize the conglomerate system tables.
			InitSystemSchema();

			// Commit the state
			StateStore.Flush();
		}

		private void InitSystemSchema() {
			using (var transaction = Database.CreateSafeTransaction(IsolationLevel.Serializable)) {
				try {
					SystemSchema.Setup(transaction);
					transaction.Commit();
				} catch (Exception ex) {
					throw new InvalidOperationException("Transaction Exception initializing tables.", ex);
				}
			}
		}

		internal void MinimalCreate() {
			if (Exists())
				throw new IOException("Composite already exists");

			// Lock the store system (generates an IOException if exclusive Lock
			// can not be made).
			if (!IsReadOnly) {
				StoreSystem.Lock(StateStoreName);
			}

			// Create/Open the state store
			stateStore = StoreSystem.CreateStore(StateStoreName);
			try {
				stateStore.Lock();

				StateStore = new TableStateStore(stateStore);
				long headP = StateStore.Create();
				// Get the fixed area
				var fixedArea = stateStore.GetArea(-1);
				fixedArea.WriteInt8(headP);
				fixedArea.Flush();
			} finally {
				stateStore.Unlock();
			}

			Setup();

			// Init the conglomerate blob store
			InitObjectStore();

			// Create the system table (but don't initialize)
			CreateSystemSchema();
		}

		private void CreateSystemSchema() {
			// Create the transaction
			ITransaction transaction = null;

			try {
				transaction = Database.CreateSafeTransaction(IsolationLevel.Serializable);
				transaction.CreateSystemSchema();

				// Commit and close the transaction.
				transaction.Commit();
				transaction = null;
			} catch (TransactionException e) {
				throw new InvalidOperationException("Transaction Exception creating composite.", e);
			} finally {
				if (transaction != null)
					transaction.Rollback();
			}
		}

		public void Close() {
			lock (commitLock) {
				CleanUp();

				StoreSystem.SetCheckPoint();

				// Go through and close all the committed tables.
				foreach (var source in tableSources.Values) {
					source.Close(false);
				}

				StateStore.Flush();
				StoreSystem.CloseStore(stateStore);

				tableSources = null;
			}

			// Release the storage system
			StoreSystem.Unlock(StateStoreName);

			if (LargeObjectStore != null)
				StoreSystem.CloseStore(lobStore);
		}

		public void Delete() {
			lock (commitLock) {
				// We possibly have things to clean up.
				CleanUp();

				// Go through and delete and close all the committed tables.
				foreach (var source in tableSources.Values)
					source.Drop();

				// Delete the state file
				StateStore.Flush();
				StoreSystem.CloseStore(stateStore);
				StoreSystem.DeleteStore(stateStore);

				// Delete the blob store
				if (LargeObjectStore != null) {
					StoreSystem.CloseStore(lobStore);
					StoreSystem.DeleteStore(lobStore);
				}

				tableSources = null;
			}

			// Release the storage system.
			StoreSystem.Unlock(StateStoreName);
		}

		ITableSource ITableSourceComposite.CreateTableSource(TableInfo tableInfo, bool temporary) {
			return CreateTableSource(tableInfo, temporary);
		}

		internal TableSource CreateTableSource(TableInfo tableInfo, bool temporary) {
			lock (commitLock) {
				try {
					int tableId = NextTableId();

					// Create the object.
					var storeSystem = StoreSystem;
					if (temporary)
						storeSystem = tempStoreSystem;

					var source = new TableSource(this, storeSystem, LargeObjectStore, tableId, tableInfo.TableName.FullName);
					source.Create(tableInfo);

					tableSources.Add(tableId, source);

					if (!temporary) {
						MarkUncommitted(tableId);

						StateStore.Flush();
					}

					// And return it.
					return source;
				} catch (IOException e) {
					throw new InvalidOperationException(String.Format("Unable to create source for table '{0}'.", tableInfo.TableName), e);
				}
			}
		}

		internal TableSource GetTableSource(int tableId) {
			lock (commitLock) {
				if (tableSources == null)
					return null;

				TableSource source;
				if (!tableSources.TryGetValue(tableId, out source))
					throw new ObjectNotFoundException(
						String.Format("Could not find any source for table with id {0} in this composite.", tableId));

				return source;
			}
		}

		public int NextTableId() {
			return StateStore.NextTableId();
		}

		private void OnCommitModification(ObjectName objName, IEnumerable<int> addedRows, IEnumerable<int> removedRows) {
			
		}

		internal void Commit(Transaction transaction, IList<ITableSource> visibleTables,
						   IEnumerable<ITableSource> selectedFromTables,
						   IEnumerable<IMutableTable> touchedTables, TransactionRegistry journal, Action<TableCommitInfo> commitActions) {

			var state = new TransactionWork(this, transaction, selectedFromTables, touchedTables, journal);

			// Exit early if nothing changed (this is a Read-only transaction)
			if (!state.HasChanges) {
				CloseTransaction(state.Transaction);
				return;
			}

			lock (commitLock) {
				var changedTablesList = state.Commit(objectStates, commitActions);

				// Flush the journals up to the minimum commit id for all the tables
				// that this transaction changed.
				long minCommitId = Database.TransactionFactory.OpenTransactions.MinimumCommitId(null);
				foreach (var master in changedTablesList) {
					master.MergeChanges(minCommitId);
				}
				int nsjsz = objectStates.Count;
				for (int i = nsjsz - 1; i >= 0; --i) {
					var namespaceJournal = objectStates[i];
					// Remove if the commit id for the journal is less than the minimum
					// commit id
					if (namespaceJournal.CommitId < minCommitId) {
						objectStates.RemoveAt(i);
					}
				}

				// Set a check point in the store system.  This means that the
				// persistance state is now stable.
				StoreSystem.SetCheckPoint();
			}
		}

		internal void Rollback(Transaction transaction, IList<IMutableTable> touchedTables, TransactionRegistry journal) {
			// Go through the journal.  Any rows added should be marked as deleted
			// in the respective master table.

			// Get individual journals for updates made to tables in this
			// transaction.
			// The list MasterTableJournal
			var journalList = new List<TableEventRegistry>();
			for (int i = 0; i < touchedTables.Count; ++i) {
				var tableJournal = touchedTables[i].EventRegistry;
				if (tableJournal.EventCount > 0) // Check the journal has entries.
					journalList.Add(tableJournal);
			}

			var changedTables = journalList.ToArray();

			lock (commitLock) {
				try {
					// For each change to each table,
					foreach (var changeJournal in changedTables) {
						// The table the changes were made to.
						int tableId = changeJournal.TableId;
						// Get the master table with this table id.
						var master = GetTableSource(tableId);
						// Commit the rollback on the table.
						master.RollbackTransactionChange(changeJournal);
					}
				} finally {
					// Notify the conglomerate that this transaction has closed.
					CloseTransaction(transaction);
				}
			}
		}

		ITableSource ITableSourceComposite.CopySourceTable(ITableSource tableSource, IIndexSet indexSet) {
			return CopySourceTable((TableSource) tableSource, indexSet);
		}

		internal TableSource CopySourceTable(TableSource tableSource, IIndexSet indexSet) {
			lock (commitLock) {
				try {
					// The unique id that identifies this table,
					int tableId = NextTableId();
					var sourceName = tableSource.SourceName;

					// Create the object.
					var masterTable = new TableSource(this, StoreSystem, LargeObjectStore, tableId, sourceName);

					masterTable.CopyFrom(tableId, tableSource, indexSet);

					// Add to the list of all tables.
					tableSources.Add(tableId, masterTable);

					// Add this to the list of deleted tables,
					MarkUncommitted(tableId);

					// Commit this
					StateStore.Flush();

					// And return it.
					return masterTable;
				} catch (IOException e) {
					throw new Exception(String.Format("Unable to copy source table '{0}' because of an error.", tableSource.TableInfo.TableName), e);
				}
			}
		}

		internal ITransaction CreateTransaction(IsolationLevel isolation) {
			var thisCommittedTables = new List<TableSource>();

			// Don't let a commit happen while we are looking at this.
			lock (commitLock) {
				int thisCommitId = CurrentCommitId;
				var committedTableList = StateStore.GetVisibleList();
				thisCommittedTables.AddRange(committedTableList.Select(resource => GetTableSource(resource.TableId)));

				// Create a set of IIndexSet for all the tables in this transaction.
				var indexInfo = (thisCommittedTables.Select(mtable => mtable.CreateIndexSet())).ToList();

                // Create a context for the transaction to handle the isolated storage of variables and services
			    var context = DatabaseContext.CreateTransactionContext();

				// Create the transaction and record it in the open transactions list.
				return new Transaction(context, Database, thisCommitId, isolation, thisCommittedTables, indexInfo);
			}
		}

		private Action<TableCommitInfo> tableCommitCallback; 

		internal void RegisterOnCommit(Action<TableCommitInfo> action) {
			if (tableCommitCallback == null) {
				tableCommitCallback = action;
			} else {
				tableCommitCallback = (Action<TableCommitInfo>) Delegate.Combine(tableCommitCallback, action);
			}
		}

		internal void UnregisterOnCommit(Action<TableCommitInfo> action) {
			tableCommitCallback = Delegate.Remove(tableCommitCallback, action) as Action<TableCommitInfo>;
		}

		internal void CloseTransaction(ITransaction transaction) {
			bool lastTransaction;
			// Closing must happen under a commit Lock.
			lock (commitLock) {
				Database.TransactionFactory.OpenTransactions.RemoveTransaction(transaction);
				// Increment the commit id.
				++CurrentCommitId;
				// Was that the last transaction?
				lastTransaction = Database.TransactionFactory.OpenTransactions.Count == 0;
			}

			// If last transaction then schedule a clean up event.
			if (lastTransaction) {
				try {
					CleanUp();
				} catch (IOException) {
					// TODO: Register the error ...
				}
			}
		}

		internal void CommitToTables(IEnumerable<int> createdTables, IEnumerable<int> droppedTables) {
			// Add created tables to the committed tables list.
			foreach (int createdTable in createdTables) {
				// For all created tables, add to the visible list and remove from the
				// delete list in the state store.
				var t = GetTableSource(createdTable);
				var resource = new TableStateStore.TableState(t.TableId, t.SourceName);
				StateStore.AddVisibleResource(resource);
				StateStore.RemoveDeleteResource(resource.SourceName);
			}

			// Remove dropped tables from the committed tables list.
			foreach (int droppedTable in droppedTables) {
				// For all dropped tables, add to the delete list and remove from the
				// visible list in the state store.
				var t = GetTableSource(droppedTable);
				var resource = new TableStateStore.TableState(t.TableId, t.SourceName);
				StateStore.AddDeleteResource(resource);
				StateStore.RemoveVisibleResource(resource.SourceName);
			}

			try {
				StateStore.Flush();
			} catch (IOException e) {
				throw new InvalidOperationException("IO Error: " + e.Message, e);
			}
		}

		internal bool ContainsVisibleResource(int resourceId) {
			return StateStore.ContainsVisibleResource(resourceId);
		}
	}
}
