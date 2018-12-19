using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

using Deveel.Data.Configurations;
using Deveel.Data.Events;
using Deveel.Data.Sql.Indexes;
using Deveel.Data.Storage;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Tables {
	public sealed class TableSource : ITableSource {
		private IndexSetStore indexSetStore;

		private FixedRecordList recordList;
		private long indexHeaderOffset;
		private long listHeaderOffset;
		private IArea headerArea;
		private long firstDeleteChainRecord;

		private long sequenceId;

		private bool isClosed;
		private int rootLock;

		internal TableSource(TableSystemV2 tableSystem, IStoreSystem storeSystem, int tableId, string sourceName) {
			TableSystem = tableSystem;
			StoreSystem = storeSystem;
			TableId = tableId;
			SourceName = sourceName;

			GC = new TableSourceGC(this);

			StoreName = MakeStoreName(tableId, sourceName);
		}

		private IDatabase Database => TableSystem.Database;

		public bool IsReadOnly => Database.IsReadOnly();

		private TableSystemV2 TableSystem { get; }

		private IStoreSystem StoreSystem { get; }

		public IStore Store { get; private set; }

		public string StoreName { get; }

		public string SourceName { get; }

		private IObjectStore ObjectStore => TableSystem.LargeObjectStore;

		private ITableFieldCache FieldCache => TableSystem.FieldCache;

		public bool CacheFields => FieldCache != null;

		private TableSourceGC GC { get; }

		public int TableId { get; set; }

		public TableInfo TableInfo { get; private set; }

		public ObjectName TableName => TableInfo.TableName;

		public IndexSetInfo IndexSetInfo { get; private set; }

		private bool HasShutdown => Database.Status == DatabaseStatus.InShutdown;

		public VersionedTableEventRegistry Registries { get; set; }

		public Task<long> GetCurrentUniqueIdAsync() {
			throw new NotImplementedException();
		}

		public Task SetUniqueIdAsync(long value) {
			throw new NotImplementedException();
		}

		public Task<long> GetNextUniqueIdAsync() {
			throw new NotImplementedException();
		}

		public IMutableTable GetMutableTable(ITransaction transaction) {
			throw new NotImplementedException();
		}

		public IMutableTable GetMutableTable(ITransaction transaction, ITableEventRegistry registry) {
			throw new NotImplementedException();
		}

		public IRowIndexSet CreateRowIndexSet() {
			throw new NotImplementedException();
		}

		public void BuildIndex() {
			throw new NotImplementedException();
		}

		public void Rollback(ITableEventRegistry registry) {
			throw new NotImplementedException();
		}

		public bool IsRootLocked {
			get {
				lock (this) {
					return rootLock > 0;
				}
			}
		}

		public long RawRowCount {
			get {
				lock (recordList) {
					return recordList.NodeCount;
				}
			}
		}

		public bool IsClosed {
			get {
				lock (this) {
					return isClosed;
				}
			}
			protected set {
				lock (this) {
					isClosed = value;
				}
			}
		}

		public bool HasChangesPending {
			get {
				lock (this) {
					return Registries.HasChanges;
				}
			}
		}

		private static string MakeStoreName(int tableId, string tableName) {
			string str = tableName.Replace('.', '_').ToLower();

			// Go through each character and remove each non a-z,A-Z,0-9,_ character.
			// This ensure there are no strange characters in the file name that the
			// underlying OS may not like.
			var name = new StringBuilder();
			int count = 0;

			for (int i = 0; i < str.Length || count > 64; ++i) {
				char c = str[i];

				if ((c >= 'a' && c <= 'z') ||
				    (c >= 'A' && c <= 'Z') ||
				    (c >= '0' && c <= '9') ||
				    c == '_') {
					name.Append(c);
					++count;
				}
			}

			return $"{tableId}_{name}";
		}

		private void SetTableOpen() {
			lock (this) {
				isClosed = false;
			}
		}

		private void SetTableInfo(TableInfo info) {
			lock (this) {
				// Check table_id isn't too large.
				if ((TableId & 0x0F0000000) != 0)
					throw new InvalidOperationException("'table_id' exceeds maximum possible keys.");

				TableInfo = info;

				// Create table indices
				Registries = new VersionedTableEventRegistry();

				// Setup the DataIndexSetInfo
				SetIndexSetInfo();
			}
		}

		private void SetIndexSetInfo() {
			lock (this) {
				// Create the initial DataIndexSetInfo object.
				IndexSetInfo = new IndexSetInfo(TableInfo.TableName);

				foreach (var colInfo in TableInfo.Columns) {
					if (colInfo.IsIndexable) {
						var indexName = $"IDX_{colInfo.ColumnName}";
						IndexSetInfo.Indexes.Add(new IndexInfo(indexName, TableInfo.TableName,
							new[] {colInfo.ColumnName}, false));
					}
				}
			}
		}

		private void ReleaseObjects() {
			lock (recordList) {
				long elements = recordList.NodeCount;

				for (long rowNumber = 0; rowNumber < elements; ++rowNumber) {
					var a = recordList.GetRecord(rowNumber);
					var status = (RecordState) a.ReadInt32();

					// Is the record not deleted?
					if (status != RecordState.Deleted) {
						// Get the record pointer
						long recordPointer = a.ReadInt64();
						ReleaseRowObjects(recordPointer);
					}
				}
			}
		}

		private void ReleaseRowObjects(long recordPointer) {
			// NOTE: Does this need to be optimized?
			IArea recordArea = Store.GetArea(recordPointer);
			recordArea.ReadInt32(); // reserved

			// Look for any blob references input the row
			for (int i = 0; i < TableInfo.Columns.Count; ++i) {
				int ctype = recordArea.ReadInt32();
				int cellOffset = recordArea.ReadInt32();

				if (ctype == 1) {
					// Type 1 is not a large object
				} else if (ctype == 2) {
					var curP = recordArea.Position;
					recordArea.Position = cellOffset + 4 + (TableInfo.Columns.Count * 8);

					int btype = recordArea.ReadInt32();
					recordArea.ReadInt32(); // (reserved)

					if (btype == 0) {
						long blobRefId = recordArea.ReadInt64();
						var obj = ObjectStore.GetObject(blobRefId);

						// Release this reference
						obj.Release();
					}

					// Revert the area pointer
					recordArea.Position = curP;
				} else {
					throw new Exception("Unrecognized type.");
				}
			}
		}

		private void ClearLocks() {
			lock (this) {
				rootLock = 0;
			}
		}

		internal void Close(bool dropPending) {
			if (IsClosed)
				return;

			lock (this) {
				// NOTE: This method MUST be synchronized over the table to prevent
				//   establishing a root Lock on this table.  If a root Lock is established
				//   then the collection event could fail.

				lock (recordList) {
					// If we are root locked, we must become un root locked.
					ClearLocks();

					try {
						try {
							Store.Lock();

							// Force a garbage collection event.
							if (!IsReadOnly)
								GC.Collect(true);

							// If we are closing pending a drop, we need to remove all blob
							// references input the table.
							// NOTE: This must only happen after the above collection event.
							if (dropPending) {
								// Scan and remove all blob references for this dropped table.
								ReleaseObjects();
							}
						} finally {
							Store.Unlock();
						}
					} catch (Exception) {
						// TODO: Register the error to the logs 
					}

					// Synchronize the store
					indexSetStore.Close();

					// Close the store input the store system.
					StoreSystem.CloseStore(Store);

					// TableInfo = null;
					IsClosed = true;
				}
			}
		}

		private void CreateTable() {
			// Initially set the table sequence_id to 1
			sequenceId = 1;

			// Create and open the store.
			// TODO: have a table-level configuration?
			Store = StoreSystem.CreateStore(StoreName, new Configuration());

			try {
				Store.Lock();

				// Setup the list structure
				recordList = new FixedRecordList(Store, 12);
			} finally {
				Store.Unlock();
			}

			// Initialize the store to an empty state,
			SetupInitialStore();
			indexSetStore.PrepareIndexes(TableInfo.Columns.Count + 1, 1, 1024);
		}

		private void SetupInitialStore() {
			byte[] tableInfoBuf;

			using (var stream = new MemoryStream()) {
				var writer = new BinaryWriter(stream, Encoding.Unicode);
				writer.Write(1);
				TableInfoSerializer.Serialize(TableInfo, writer);

				tableInfoBuf = stream.ToArray();
			}

			byte[] indexSetInfoBuf;

			using (var stream = new MemoryStream()) {
				var writer = new BinaryWriter(stream, Encoding.Unicode);
				writer.Write(1);

				IndexSetInfoSerializer.Serialize(IndexSetInfo, writer);

				indexSetInfoBuf = stream.ToArray();
			}

			try {
				Store.Lock();

				// Allocate an 80 byte header
				using (var headerWriter = Store.CreateArea(80)) {
					long headerPointer = headerWriter.Id;

					// Allocate space to store the DataTableInfo serialization
					using (var dataTableDefWriter = Store.CreateArea(tableInfoBuf.Length)) {
						long tableInfoOffset = dataTableDefWriter.Id;

						// Allocate space to store the DataIndexSetInfo serialization
						using (var indexSetWriter = Store.CreateArea(indexSetInfoBuf.Length)) {
							long indexSetInfoPointer = indexSetWriter.Id;

							// Allocate space for the list header
							listHeaderOffset = recordList.Create();
							recordList.WriteDeleteHead(-1);
							firstDeleteChainRecord = -1;

							// Create the index store
							indexSetStore = new IndexSetStore(Store);
							indexHeaderOffset = indexSetStore.Create();

							// Write the main header
							headerWriter.Write((int) 1); // Version
							headerWriter.Write(TableId); // table id
							headerWriter.Write(sequenceId); // initial sequence id
							headerWriter.Write(tableInfoOffset); // pointer to DataTableInfo
							headerWriter.Write(indexSetInfoPointer); // pointer to DataIndexSetInfo
							headerWriter.Write(indexHeaderOffset); // index header pointer
							headerWriter.Write(listHeaderOffset); // list header pointer
							headerWriter.Flush();

							// Write the table info
							dataTableDefWriter.Write(tableInfoBuf, 0, tableInfoBuf.Length);
							dataTableDefWriter.Flush();

							// Write the index set info
							indexSetWriter.Write(indexSetInfoBuf, 0, indexSetInfoBuf.Length);
							indexSetWriter.Flush();

							// Set the pointer to the header input the reserved area.
							using (var fixedArea = Store.GetArea(-1)) {
								fixedArea.Write(headerPointer);
								fixedArea.Flush();
							}

							// Set the header area
							headerArea = Store.GetArea(headerPointer);
						}
					}
				}
			} finally {
				Store.Unlock();
			}
		}

		private RecordState ReadRecordState(long rowNumber) {
			lock (recordList) {
				// Find the record entry input the block list.
				var blockArea = recordList.GetRecord(rowNumber);

				// Get the status.
				return (RecordState) blockArea.ReadInt32();
			}
		}

		private RecordState WriteRecordState(long rowNumber, RecordState state) {
			lock (recordList) {
				if (HasShutdown)
					throw new IOException("IO operation while shutting down.");

				// Find the record entry input the block list.
				var blockArea = recordList.GetRecord(rowNumber);
				var pos = blockArea.Position;
				// Get the status.
				var oldStatus = (RecordState) blockArea.ReadInt32();

				// Write the new status
				try {
					Store.Lock();

					blockArea.Position = pos;
					blockArea.Write((int)state);
					blockArea.Flush();
				} finally {
					Store.Unlock();
				}

				return oldStatus;
			}
		}

		private void CheckForCleanup() {
			lock (this) {
				GC.Collect(false);
			}
		}

		private bool IsRecordDeleted(long rowNumber) {
			var state = ReadRecordState(rowNumber);

			return state == RecordState.Deleted;
		}

		private void DoHardRowRemove(long rowNumber) {
			lock (this) {
				// Internally delete the row,
				OnDeleteRow(rowNumber);

				// Update stats
				// TODO: Database.RaiseEvent<RowDeleteEvent>(TableInfo.TableName, TableId, rowNumber);
			}
		}

		private void OnDeleteRow(long rowIndex) {
			lock (recordList) {
				if (HasShutdown)
					throw new IOException("IO operation while VM shutting down.");

				// Find the record entry input the block list.
				var blockArea = recordList.GetRecord(rowIndex);
				var p = blockArea.Position;
				var status = (RecordState) blockArea.ReadInt32();

				// Check it is not already deleted
				if (status == RecordState.Deleted)
					throw new IOException("Record is already marked as deleted.");

				long recordPointer = blockArea.ReadInt64();

				// Update the status record.
				try {
					Store.Lock();

					blockArea.Position = p;
					blockArea.Write((int) RecordState.Deleted);
					blockArea.Write(firstDeleteChainRecord);
					blockArea.Flush();
					firstDeleteChainRecord = rowIndex;

					// Update the first_delete_chain_record field input the header
					recordList.WriteDeleteHead(firstDeleteChainRecord);

					// If the record contains any references to blobs, remove the reference
					// here.
					ReleaseRowObjects(recordPointer);

					// Free the record from the store
					Store.DeleteArea(recordPointer);
				} finally {
					RemoveRowFromCache(rowIndex);
					Store.Unlock();
				}
			}
		}

		private void RemoveRowFromCache(long rowIndex) {
			if (CacheFields) {
				var colCount = TableInfo.Columns.Count;

				for (int i = 0; i < colCount; i++) {
					FieldCache.Remove(TableInfo.TableName, rowIndex, i);
				}
			}
		}

		private void CommitIndexSet(IRowIndexSet indexSet) {
			indexSetStore.CommitIndexSet(indexSet);
			indexSet.Dispose();
		}

		internal IEnumerable<ITableEventRegistry> FindChangesSinceCommit(long commitId) {
			lock (this) {
				return Registries.FindSinceCommit(commitId);
			}
		}

		internal void CommitTransactionChange(long commitId, ITableEventRegistry change, IRowIndexSet indexSet) {
			lock (this) {
				// ASSERT: Can't do this if source is Read only.
				if (IsReadOnly)
					throw new InvalidOperationException("Can't commit transaction journal, table is Read only.");

				// CHECK!
				// TODO: change.CommitId = commitId;

				try {
					// Add this registry to the multi version table indices log
					Registries.AddRegistry(change);

					// Write the modified index set to the index store
					// (Updates the index file)
					CommitIndexSet(indexSet);

					// Update the state of the committed added data to the file system.
					// (Updates data to the allocation file)
					//
					// ISSUE: This can add up to a lot of changes to the allocation file and
					//   the runtime could potentially be terminated in the middle of
					//   the update.  If an interruption happens the allocation information
					//   may be incorrectly flagged.  The type of corruption this would
					//   result in would be;
					//   + From an 'update' the updated record may disappear.
					//   + From a 'delete' the deleted record may not delete.
					//   + From an 'insert' the inserted record may not insert.
					//
					// Note, the possibility of this type of corruption occuring has been
					// minimized as best as possible given the current architecture.
					// Also note that is not possible for a table file to become corrupted
					// beyond recovery from this issue.

					foreach (var entry in change) {
						if (entry is TableRowEvent) {
							var rowEvent = (TableRowEvent) entry;
							var rowIndex = rowEvent.RowNumber;

							if (rowEvent.EventType == TableRowEventType.Add) {
								// Record commit added
								var oldType = WriteRecordState(rowIndex, RecordState.CommittedAdded);

								// Check the record was in an uncommitted state before we changed
								// it.
								if (oldType != RecordState.Uncommitted) {
									WriteRecordState(rowIndex, oldType);
									throw new InvalidOperationException($"Record {rowIndex} of table {TableName} was not in an uncommitted state!");
								}
							} else if (rowEvent.EventType == TableRowEventType.Remove) {
								// Record commit removed
								var oldType = WriteRecordState(rowIndex, RecordState.CommittedRemoved);

								// Check the record was in an added state before we removed it.
								if (oldType != RecordState.CommittedAdded) {
									WriteRecordState(rowIndex, oldType);
									throw new InvalidOperationException($"Record {rowIndex} of table {TableName} was not in an added state!");
								}

								// Notify collector that this row has been marked as deleted.
								GC.DeleteRow(rowIndex);
							}
						}
					}
				} catch (IOException e) {
					throw new InvalidOperationException("IO Error: " + e.Message, e);
				}

			}
		}

		internal void HardRemoveRow(long rowIndex) {
			lock (this) {
				// ASSERTION: We are not under a root Lock.
				if (IsRootLocked)
					throw new InvalidOperationException("Cannot remove row, table is locked");

				var typeKey = ReadRecordState(rowIndex);

				// Check this record is marked as committed removed.
				if (typeKey != RecordState.CommittedRemoved)
					throw new InvalidOperationException($"The row {rowIndex} is not marked as committed removed");

				DoHardRowRemove(rowIndex);
			}
		}

		internal bool HardCheckAndReclaimRow(int recordIndex) {
			lock (this) {
				// ASSERTION: We are not under a root Lock.
				if (IsRootLocked)
					throw new InvalidOperationException(
						"Assertion failed: Can't remove row, table is under a root lock.");

				// Row already deleted?
				if (IsRecordDeleted(recordIndex))
					return false;

				var typeKey = ReadRecordState(recordIndex);

				// Check this record is marked as committed removed.
				if (typeKey != RecordState.CommittedRemoved)
					return false;

				DoHardRowRemove(recordIndex);

				return true;
			}
		}



		internal bool Drop() {
			lock (this) {
				lock (recordList) {
					if (!IsClosed)
						Close(true);

					if (StoreSystem.DeleteStore(Store)) {
						// TODO: log this
						return true;
					}

					return false;
				}
			}
		}

		public void Create(TableInfo tableInfo) {
			// Set the data table info object
			SetTableInfo(tableInfo);

			// Load internal state
			SetTableOpen();

			// Set up internal state of this object
			//TableId = tableInfo.Id;

			CreateTable();
		}

		public void Dispose() {
			if (FieldCache != null)
				FieldCache.Clear();

			if (headerArea != null)
				headerArea.Dispose();

			if (recordList != null)
				recordList.Dispose();

			if (Registries != null)
				Registries.Dispose();

			if (indexSetStore != null)
				indexSetStore.Dispose();

			if (Store != null) {
				if (StoreSystem.CloseStore(Store))
					Store.Dispose();
			}

			headerArea = null;
			recordList = null;
			Registries = null;
			indexSetStore = null;

		}

		internal void RollbackTransactionChange(ITableEventRegistry registry) {
			lock (this) {
				// ASSERT: Can't do this is source is Read only.
				if (IsReadOnly)
					throw new InvalidOperationException("Can't rollback transaction journal, table is Read only.");

				// Any rows added in the journal are marked as committed deleted and the
				// journal is then discarded.

				try {
					// Mark all rows in the data_store as appropriate to the changes.
					foreach (var tableEvent in registry) {
						if (tableEvent is TableRowEvent) {
							var rowEvent = (TableRowEvent) tableEvent;

							if (rowEvent.EventType == TableRowEventType.Add) {
								var oldState = WriteRecordState(rowEvent.RowNumber, RecordState.CommittedRemoved);

								if (oldState != RecordState.Uncommitted) {
									WriteRecordState(rowEvent.RowNumber, oldState);

									throw new InvalidOperationException($"Record {rowEvent.RowNumber} was not in an uncommitted state.");
								}

								GC.DeleteRow(rowEvent.RowNumber);
							}
						}
					}
				} catch (IOException e) {
					throw new InvalidOperationException("IO Error: " + e.Message, e);
				}
			}
		}

		internal void MergeChanges(long commitId) {
			lock (this) {
				bool allMerged = Registries.MergeChanges(commitId);
				// If all journal entries merged then schedule deleted row collection.
				if (allMerged && !IsReadOnly) {
					CheckForCleanup();
				}
			}
		}
	}
}