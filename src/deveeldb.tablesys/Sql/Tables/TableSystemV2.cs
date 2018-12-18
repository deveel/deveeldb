// 
//  Copyright 2010-2018 Deveel
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

using Deveel.Data.Configurations;
using Deveel.Data.Services;
using Deveel.Data.Storage;

namespace Deveel.Data.Sql.Tables {
	public sealed class TableSystemV2 : ITableSystem, IDisposable {
		private readonly object commitLock = new object();
		private Dictionary<int, TableSource> tableSources;

		private IStoreSystem tempStoreSystem;
		private IStore lobStore;
		private IStore stateStore;

		private const string StateStorePostfix = "_sf";
		private const string LobPostfix = ".lob";

		public TableSystemV2(IDatabase database, IStoreSystem storeSystem) {
			Database = database ?? throw new ArgumentNullException(nameof(database));
			StoreSystem = storeSystem ?? throw new ArgumentNullException(nameof(storeSystem));

			StateStoreName = $"{database.Name}{StateStorePostfix}";
			ObjectStoreName = $"{database.Name}{LobPostfix}";

			// TODO: Database.Consume(OnDatabaseEvent);

			Setup();
		}

		public IDatabase Database { get; set; }

		public IStoreSystem StoreSystem { get; }

		private string StateStoreName { get; }

		public bool IsReadOnly => Database.IsReadOnly();

		internal LargeObjectStore LargeObjectStore { get; set; }

		public bool IsClosed { get; private set; }

		private TableStateStore StateStore { get; set; }

		private string ObjectStoreName { get; }

		internal ITableFieldCache FieldCache => Database.Scope.Resolve<ITableFieldCache>();

		public bool Exists => StoreSystem.StoreExists(StateStoreName);

		private void Setup() {
			lock (this) {
				tableSources = new Dictionary<int, TableSource>();
				IsClosed = false;
			}
		}

		private void MinimalCreate() {
			if (Exists)
				throw new IOException("Composite already exists");

			// Lock the store system (generates an IOException if exclusive Lock
			// can not be made).
			if (!IsReadOnly) {
				StoreSystem.Lock(StateStoreName);
			}

			// Create/Open the state store
			// TODO: Support store-level configuration?
			stateStore = StoreSystem.CreateStore(StateStoreName, new Configuration());
			try {
				stateStore.Lock();

				StateStore = new TableStateStore(stateStore);
				long headP = StateStore.Create();
				// Get the fixed area
				var fixedArea = stateStore.GetArea(-1);
				fixedArea.Write(headP);
				fixedArea.Flush();
			} finally {
				stateStore.Unlock();
			}

			Setup();

			// Init the conglomerate blob store
			InitObjectStore();

			// Create the system table (but don't initialize)
			CreateSystem();
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
			// TODO: Support store-level configuration?
			if (blobStoreExists) {
				lobStore = StoreSystem.OpenStore(ObjectStoreName, new Configuration());
			} else {
				lobStore = StoreSystem.CreateStore(ObjectStoreName, new Configuration());
			}

			try {
				lobStore.Lock();

				// TODO: have multiple BLOB stores
				LargeObjectStore = new LargeObjectStore(0, lobStore);

				// Get the 64 byte fixed area
				var fixedArea = lobStore.GetArea(-1, false);
				// If the blob store didn't exist then we need to create it here,
				if (!blobStoreExists) {
					long headerP = LargeObjectStore.Create();
					fixedArea.Write(headerP);
					fixedArea.Flush();
				} else {
					// Otherwise we need to initialize the blob store
					long headerP = fixedArea.ReadInt64();
					LargeObjectStore.Open(headerP);
				}
			} finally {
				lobStore.Unlock();
			}
		}

		private void CreateSystem() {
			// TODO: Configure the System Schema
		}

		private void SetupSystem() {
			// TODO: Setup the System Schema
		}

		private int NextTableId() {
			return StateStore.NextTableId();
		}

		private void MarkUncommitted(int tableId) {
			var tableSource = GetTableSource(tableId);
			StateStore.AddDeleteResource(new TableStateStore.TableState(tableId, tableSource.SourceName));
		}

		private void CleanUp() {
			lock (commitLock) {
				if (IsClosed)
					return;

				// If no open transactions on the database, then clean up.
				if (Database.OpenTransactions == null ||
				    Database.OpenTransactions.Count == 0) {
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
				if (source.SourceName.Equals(tableFileName)) {
					if (source.IsRootLocked)
						return false;

					if (!source.Drop())
						return false;

					tableId = source.TableId;
				}
			}

			if (tableId != null)
				tableSources.Remove(tableId.Value);

			return true;
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

		public void Create() {
			MinimalCreate();

			// Initialize the conglomerate system tables.
			SetupSystem();

			// Commit the state
			StateStore.Flush();
		}

		public void Delete() {
			throw new NotImplementedException();
		}

		public void Open() {
			throw new NotImplementedException();
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

				//tableSources = null;
				IsClosed = true;
			}

			// Release the storage system
			StoreSystem.Unlock(StateStoreName);

			if (LargeObjectStore != null)
				StoreSystem.CloseStore(lobStore);
		}

		public ITableSource CreateTableSource(TableInfo tableInfo, bool temporary) {
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
					throw new InvalidOperationException($"Unable to create source for table '{tableInfo.TableName}'.", e);
				}
			}
		}

		public TableSource GetTableSource(int tableId) {
			lock (commitLock) {
				if (tableSources == null)
					return null;

				if (!tableSources.TryGetValue(tableId, out var source))
					throw new InvalidOperationException($"Could not find any source for table with id {tableId} in this composite.");

				return source;
			}
		}

		ITableSource ITableSystem.GetTableSource(int tableId)
			=> GetTableSource(tableId);

		public IEnumerable<ITableSource> GetTableSources() {
			throw new NotImplementedException();
		}

		public void Dispose() {
			if (!IsClosed)
				Close();

			if (lobStore != null)
				lobStore.Dispose();
			if (stateStore != null)
				stateStore.Dispose();

			if (tableSources != null) {
				foreach (var tableSource in tableSources) {
					tableSource.Value.Dispose();
				}

				tableSources.Clear();
			}

			if (StateStore != null)
				StateStore.Dispose();

			if (LargeObjectStore != null)
				LargeObjectStore.Dispose();

			if (tempStoreSystem != null)
				tempStoreSystem.Dispose();

			tableSources = null;
			StateStore = null;
			tempStoreSystem = null;
			lobStore = null;
			Database = null;
		}
	}
}