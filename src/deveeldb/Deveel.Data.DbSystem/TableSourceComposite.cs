using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Deveel.Data.Deveel.Data.Transactions;
using Deveel.Data.Index;
using Deveel.Data.Sql;
using Deveel.Data.Store;
using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	public sealed class TableSourceComposite : IDisposable {
		private readonly object commitLock = new object();
		private Dictionary<int, TableSource> tableSources;

		private IStoreSystem tempStoreSystem;
		private IStore lobStore;
		private IStore stateStore;

		private const string StateStorePostfix = "_sf";

		public const string ObjectStoreName = "lob_store";

		public TableSourceComposite(ISystemContext systemContext, IDatabase database) {
			SystemContext = systemContext;
			Database = database;

			tempStoreSystem = new InMemoryStorageSystem();

			StateStoreName = String.Format("{0}_{1}", database.Name, StateStorePostfix);

			Setup();
		}

		~TableSourceComposite() {
			Dispose(false);
		}

		public IDatabase Database { get; private set; }

		public ISystemContext SystemContext { get; private set; }

		public IStoreSystem StoreSystem {
			get { return SystemContext.StoreSystem; }
		}

		public int CurrentCommitId { get; private set; } 

		public bool IsReadOnly {
			get {
				//TODO: return SystemContext.IsReadOnly();
				return false;
			}
		}

		public bool IsClosed {
			get { return tableSources == null; }
		}

		private TableStateStore StateStore { get; set; }

		private string StateStoreName { get; set; }

		public IObjectStore LargeObjectStore { get; private set; }

		private void ReadVisibleTables() {
			lock (commitLock) {
				var tables = StateStore.GetVisibleList();

				// For each visible table
				foreach (var resource in tables) {
					var tableId = resource.TableId;
					var tableName = resource.TableName;

					// TODO: add a table source type?

					// Load the master table from the resource information
					var source = LoadTableSource(tableId, tableName);

					if (source == null)
						throw new InvalidOperationException(String.Format("Table {0} was not found.", tableName));

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
					string tableName = resource.TableName;

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
			var source = new TableSource(this, StoreSystem, tableId, tableName);
			if (!source.Exists())
				return null;

			return source;
		}

		private void MarkUncommitted(int tableId) {
			var masterTable = GetTableSource(tableId);
			StateStore.AddDeleteResource(new TableStateStore.TableState(tableId, masterTable.TableName));
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

				lobStore = null;
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Setup() {
			CurrentCommitId = 0;
			tableSources = new Dictionary<int, TableSource>();
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
				lobStore.LockForWrite();

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
				lobStore.UnlockForWrite();
			}
		}

		private void CleanUp() {
			lock (commitLock) {
				if (IsClosed)
					return;

				// If no open transactions on the database, then clean up.
				if (Database.OpenTransactions.Count == 0) {
					var deleteList = StateStore.GetDeleteList().ToArray();
					if (deleteList.Length > 0) {
						int dropCount = 0;

						for (int i = deleteList.Length - 1; i >= 0; --i) {
							var tableName = deleteList[i].TableName;
							CloseTable(tableName, true);
						}

						for (int i = deleteList.Length - 1; i >= 0; --i) {
							string tableName = deleteList[i].TableName;
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
				if (source.TableName.Equals(tableFileName)) {
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

		private void CloseTable(string tableName, bool pendingDrop) {
			// Find the table with this file name.
			foreach (var source in tableSources.Values) {
				if (source.TableName.Equals(tableName)) {
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
			// Create the transaction
			ITransaction transaction = null;

			try {
				transaction = Database.CreateTransaction(TransactionIsolation.Serializable);
				SystemSchema.Setup(transaction);

				transaction.Commit();
				transaction = null;
			} catch (Exception ex) {
				throw new ApplicationException("Transaction Exception initializing tables.", ex);
			} finally {
				if (transaction != null)
					transaction.Rollback();
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
				stateStore.LockForWrite();

				StateStore = new TableStateStore(stateStore);
				long headP = StateStore.Create();
				// Get the fixed area
				var fixedArea = stateStore.GetArea(-1);
				fixedArea.WriteInt8(headP);
				fixedArea.Flush();
			} finally {
				stateStore.UnlockForWrite();
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
				transaction = Database.CreateTransaction(TransactionIsolation.Serializable);
				transaction.CreateSystemSchema();

				// Commit and close the transaction.
				transaction.Commit();
				transaction = null;
			} catch (TransactionException e) {
				throw new ApplicationException("Transaction Exception creating composite.", e);
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

			// Unlock the storage system
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

			// Unlock the storage system.
			StoreSystem.Unlock(StateStoreName);
		}

		internal TableSource CreateTableSource(TableInfo tableInfo, bool temporary) {
			lock (commitLock) {
				try {
					int tableId = NextTableId();

					// Create the object.
					var storeSystem = StoreSystem;
					if (temporary)
						storeSystem = tempStoreSystem;

					var source = new TableSource(this, storeSystem, tableId, tableInfo.TableName.FullName);
					source.Create(tableInfo);

					tableSources.Add(tableId, source);

					if (!temporary) {
						MarkUncommitted(tableId);

						StateStore.Flush();
					}

					// And return it.
					return source;
				} catch (IOException e) {
					throw new ApplicationException(String.Format("Unable to create source for table '{0}'.", tableInfo.TableName), e);
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

		public void Commit(TransactionState state) {
			
		}

		public void Rollback(TransactionState state) {
			
		}

		internal TableSource CopySourceTable(TableSource tableSource, IIndexSet indexSet) {
			lock (commitLock) {
				try {
					// The unique id that identifies this table,
					int tableId = NextTableId();
					var tableName = tableSource.TableName;

					// Create the object.
					var masterTable = new TableSource(this, StoreSystem, tableId, tableName);

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
	}
}
