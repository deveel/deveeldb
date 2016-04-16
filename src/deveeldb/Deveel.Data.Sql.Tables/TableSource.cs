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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

using Deveel.Data.Caching;
using Deveel.Data.Index;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Store;
using Deveel.Data.Transactions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Tables {
	class TableSource : ITableSource {
		private IndexSetStore indexSetStore;
		private VersionedTableIndexList tableIndices;

		private FixedRecordList recordList;
		private long indexHeaderOffset;
		private long listHeaderOffset;
		private IArea headerArea;
		private long firstDeleteChainRecord;

		private long sequenceId;

		private bool isClosed;
		private int rootLock;

		internal TableSource(TableSourceComposite composite, IStoreSystem storeSystem, IObjectStore objStore, int tableId, string sourceName) {
			if (composite == null)
				throw new ArgumentNullException("composite");

			Composite = composite;
			StoreSystem = storeSystem;
			ObjectStore = objStore;
			TableId = tableId;
			SourceName = sourceName;

			GC = new TableSourceGC(this);

			CellCache = composite.DatabaseContext.ResolveService<ITableCellCache>();

			// Generate the name of the store file name.
			StoreIdentity = MakeSourceIdentity(composite.DatabaseContext.SystemContext, tableId, sourceName);
		}

		~TableSource() {
			Dispose(false);
		}

		public TableSourceComposite Composite { get; private set; }

		public IDatabaseContext DatabaseContext {
			get { return Composite.DatabaseContext; }
		}

		public IDatabase Database {
			get { return Composite.Database; }
		}

		public ISystemContext SystemContext {
			get { return DatabaseContext.SystemContext; }
		}

		private IStoreSystem StoreSystem { get; set; }

		public int TableId { get; private set; }

		public ObjectName TableName {
			get { return TableInfo.TableName; }
		}

		public string SourceName { get; private set; }

		public bool IsRootLocked {
			get {
				lock (this) {
					return rootLock > 0;
				}
			}
		}

		public TableInfo TableInfo { get; private set; }

		public int ColumnCount {
			get { return TableInfo.ColumnCount; }
		}

		public int RawRowCount {
			get {
				lock (recordList) {
					long total = recordList.NodeCount;
					// 32-bit row limitation here - we should return a long.
					return (int)total;
				}
			}
		}

		public long CurrentUniqueId {
			get {
				lock (recordList) {
					return sequenceId - 1;
				}
			}
		}

		public bool CanCompact {
			get {
				// TODO: We should perform some analysis on the data to decide if a
				//   compact is necessary or not.
				return true;
			}
		}

		public bool IsReadOnly {
			get { return DatabaseContext.SystemContext.ReadOnly(); }
		}

		public IndexSetInfo IndexSetInfo { get; private set; }

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
					return tableIndices.HasChangesPending;
				}
			}
		}

		public string StoreIdentity { get; private set; }

		public IStore Store { get; private set; }

		public IObjectStore ObjectStore { get; private set; }

		public TableSourceGC GC { get; private set; }

		public bool HasShutdown { get; private set; }

		private ITableCellCache CellCache { get; set; }

		public bool CellCaching {
			get { return CellCache != null; }
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				StoreSystem.CloseStore(Store);

				if (Store != null)
					Store.Dispose();
			}

			ObjectStore = null;
			Store = null;
		}

		public void Dispose() {
			Dispose(true);
			System.GC.SuppressFinalize(this);
		}

		public bool Exists() {
			return StoreSystem.StoreExists(StoreIdentity);
		}

		private void ClearLocks() {
			lock (this) {
				rootLock = 0;
			}
		}

		public void Close(bool dropPending) {
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

					TableInfo = null;
					IsClosed = true;
				}
			}
		}

		private void ReleaseObjects() {
			lock (recordList) {
				long elements = recordList.NodeCount;
				for (long rowNumber = 0; rowNumber < elements; ++rowNumber) {
					var a = recordList.GetRecord(rowNumber);
					var status = (RecordState) a.ReadInt4();
					// Is the record not deleted?
					if (status != RecordState.Deleted) {
						// Get the record pointer
						long recordPointer = a.ReadInt8();
						ReleaseRowObjects(recordPointer);
					}
				}
			}
		}

		public bool Drop() {
			throw new NotImplementedException();
		}

		public void Open() {
			bool needsCheck = OpenTable();

			// Create table indices
			tableIndices = new VersionedTableIndexList(this);

			// Load internal state
			LoadInternal();

			if (needsCheck) {
				// Do an opening scan of the table.  Any records that are uncommited
				// must be marked as deleted.
				DoOpeningScan();
			}
		}

		private void DoOpeningScan() {
			lock (this) {
				// ASSERTION: No root locks and no pending transaction changes,
				//   VERY important we assert there's no pending transactions.
				if (IsRootLocked || HasChangesPending)
					// This shouldn't happen if we are calling from 'open'.
					throw new Exception("Odd, we are root locked or have pending journal changes.");

				// This is pointless if we are in Read only mode.
				if (!IsReadOnly) {
					// Get the master index of rows in this table
					var indexSet = CreateIndexSet();
					var masterIndex = indexSet.GetIndex(0);

					// NOTE: We assume the index information is correct and that the
					//   allocation information is potentially bad.

					int rowCount = RawRowCount;
					for (int rowNumber = 0; rowNumber < rowCount; ++rowNumber) {
						// Is this record marked as deleted?
						if (!IsRecordDeleted(rowNumber)) {
							// Get the type flags for this record.
							var type = ReadRecordState(rowNumber);

							// Check if this record is marked as committed removed, or is an
							// uncommitted record.
							if (type == RecordState.CommittedRemoved ||
								type == RecordState.Uncommitted) {
								// Check it's not in the master index...
								if (!masterIndex.Contains(rowNumber)) {
									// Delete it.
									DoHardRowRemove(rowNumber);
								} else {
									// Mark the row as committed added because it is in the index.
									WriteRecordState(rowNumber, RecordState.CommittedAdded);

								}
							} else {
								// Must be committed added.  Check it's indexed.
								if (!masterIndex.Contains(rowNumber)) {
									// Not indexed, so data is inconsistant.

									// Mark the row as committed removed because it is not in the
									// index.
									WriteRecordState(rowNumber, RecordState.CommittedRemoved);

								}
							}
						} else {
							// if deleted
							// Check this record isn't in the master index.
							if (masterIndex.Contains(rowNumber)) {
								// It's in the master index which is wrong!  We should remake the
								// indices.

								// Mark the row as committed added because it is in the index.
								WriteRecordState(rowNumber, RecordState.CommittedAdded);

							}
						}
					} // for (int i = 0 ; i < row_count; ++i)

					// Dispose the index set
					indexSet.Dispose();
				}

				ScanForLeaks();
			}
		}

		private void ScanForLeaks() {
			lock (recordList) {
				// The list of pointers to areas (as Long).
				var usedAreas = new List<long>();

				usedAreas.Add(headerArea.Id);

				headerArea.Position = 16;
				// Add the DataTableInfo and DataIndexSetInfo objects
				usedAreas.Add(headerArea.ReadInt8());
				usedAreas.Add(headerArea.ReadInt8());

				// Add all the used areas input the list_structure itself.
				recordList.GetAreasUsed(usedAreas);

				// Adds all the user areas input the index store.
				indexSetStore.GetAreasUsed(usedAreas);

				// Search the list structure for all areas
				long elements = recordList.NodeCount;
				for (long i = 0; i < elements; ++i) {
					var area = recordList.GetRecord(i);
					var status = (RecordState) area.ReadInt4();
					if (status != RecordState.Deleted) {
						usedAreas.Add(area.ReadInt8());
					}
				}

				// Following depends on store implementation
				if (Store is StoreBase) {
					var aStore = (StoreBase)Store;
					var leakedAreas = aStore.FindAllocatedAreasNotIn(usedAreas).ToList();
					if (leakedAreas.Count == 0) {
					} else {
						foreach (long areaPointer in leakedAreas) {
							Store.DeleteArea(areaPointer);
						}
					}
				}
			}
		}

		private bool OpenTable() {
			// Open the store.
			Store = StoreSystem.OpenStore(StoreIdentity);
			bool needCheck = !Store.ClosedClean;

			// Setup the list structure
			recordList = new FixedRecordList(Store, 12);

			// Read and setup the pointers
			ReadStoreHeaders();

			return needCheck;
		}

		public IIndexSet CreateIndexSet() {
			return indexSetStore.GetSnapshotIndex();
		}

		public void AddIndex(IndexInfo indexInfo) {
			lock (this) {
				// TODO: are there other checks to be done here?

				IndexSetInfo.AddIndex(indexInfo);
			}
		}

		private void CommitIndexSet(IIndexSet indexSet) {
			indexSetStore.CommitIndexSet(indexSet);
			indexSet.Dispose();
		}

		private void SetTableInfo(TableInfo info) {
			lock (this) {
				// Check table_id isn't too large.
				if ((TableId & 0x0F0000000) != 0)
					throw new InvalidOperationException("'table_id' exceeds maximum possible keys.");

				info.Establish(TableId);
				TableInfo = info;

				// Create table indices
				tableIndices = new VersionedTableIndexList(this);

				// Setup the DataIndexSetInfo
				SetIndexSetInfo();
			}
		}

		private void SetIndexSetInfo() {
			lock (this) {
				// Create the initial DataIndexSetInfo object.
				IndexSetInfo = new IndexSetInfo(TableInfo.TableName);
				foreach (var colInfo in TableInfo) {
					if (colInfo.IsIndexable) {
						var indexName = String.Format("IDX_{0}", colInfo.ColumnName);
						var indexType = colInfo.IndexType;
						if (String.IsNullOrEmpty(indexType))
							indexType = DefaultIndexTypes.InsertSearch;

						IndexSetInfo.AddIndex(new IndexInfo(indexName, indexType, new[] {colInfo.ColumnName}, false));
					}
				}
			}
		}

		private void LoadInternal() {
			lock (this) {
				// Set up the stat keys.
				// TODO: 
				isClosed = false;
			}
		}

		public void Create(TableInfo tableInfo) {
			// Set the data table info object
			SetTableInfo(tableInfo);

			// Load internal state
			LoadInternal();

			// Set up internal state of this object
			//TableId = tableInfo.Id;

			CreateTable();
		}

		private static string MakeSourceIdentity(ISystemContext context, int tableId, string tableName) {
			string str = tableName.Replace('.', '_').ToLower();

			// Go through each character and remove each non a-z,A-Z,0-9,_ character.
			// This ensure there are no strange characters in the file name that the
			// underlying OS may not like.
			StringBuilder osifiedName = new StringBuilder();
			int count = 0;
			for (int i = 0; i < str.Length || count > 64; ++i) {
				char c = str[i];
				if ((c >= 'a' && c <= 'z') ||
					(c >= 'A' && c <= 'Z') ||
					(c >= '0' && c <= '9') ||
					c == '_') {
					osifiedName.Append(c);
					++count;
				}
			}

			return String.Format("{0}_{1}", tableId, osifiedName);
		}

		private void CreateTable() {
			// Initially set the table sequence_id to 1
			sequenceId = 1;

			// Create and open the store.
			Store = StoreSystem.CreateStore(StoreIdentity);

			try {
				Store.Lock();

				// Setup the list structure
				recordList = new FixedRecordList(Store, 12);
			} finally {
				Store.Unlock();
			}

			// Initialize the store to an empty state,
			SetupInitialStore();
			indexSetStore.PrepareIndexLists(TableInfo.ColumnCount + 1, 1, 1024);
		}

		private void SetupInitialStore() {
			byte[] tableInfoBuf;
			using (var stream = new MemoryStream()) {
				var writer = new BinaryWriter(stream, Encoding.Unicode);
				writer.Write(1);
				TableInfo.Serialize(TableInfo, writer);

				tableInfoBuf = stream.ToArray();
			}

			byte[] indexSetInfoBuf;
			using (var stream = new MemoryStream()) {
				var writer = new BinaryWriter(stream, Encoding.Unicode);
				writer.Write(1);

				IndexSetInfo.SerialiazeTo(stream);
				
				indexSetInfoBuf = stream.ToArray();
			}

			try {
				Store.Lock();

				// Allocate an 80 byte header
				var headerWriter = Store.CreateArea(80);
				long headerPointer = headerWriter.Id;
				// Allocate space to store the DataTableInfo serialization
				var dataTableDefWriter = Store.CreateArea(tableInfoBuf.Length);
				long tableInfoOffset = dataTableDefWriter.Id;
				// Allocate space to store the DataIndexSetInfo serialization
				var indexSetWriter = Store.CreateArea(indexSetInfoBuf.Length);
				long indexSetInfoPointer = indexSetWriter.Id;

				// Allocate space for the list header
				listHeaderOffset = recordList.Create();
				recordList.WriteDeleteHead(-1);
				firstDeleteChainRecord = -1;

				// Create the index store
				indexSetStore = new IndexSetStore(DatabaseContext, Store);
				indexHeaderOffset = indexSetStore.Create();

				// Write the main header
				headerWriter.WriteInt4(1);                       // Version
				headerWriter.WriteInt4(TableId);                 // table id
				headerWriter.WriteInt8(sequenceId);              // initial sequence id
				headerWriter.WriteInt8(tableInfoOffset);     // pointer to DataTableInfo
				headerWriter.WriteInt8(indexSetInfoPointer);  // pointer to DataIndexSetInfo
				headerWriter.WriteInt8(indexHeaderOffset);      // index header pointer
				headerWriter.WriteInt8(listHeaderOffset);       // list header pointer
				headerWriter.Flush();

				// Write the table info
				dataTableDefWriter.Write(tableInfoBuf, 0, tableInfoBuf.Length);
				dataTableDefWriter.Flush();

				// Write the index set info
				indexSetWriter.Write(indexSetInfoBuf, 0, indexSetInfoBuf.Length);
				indexSetWriter.Flush();

				// Set the pointer to the header input the reserved area.
				var fixedArea = Store.GetArea(-1);
				fixedArea.WriteInt8(headerPointer);
				fixedArea.Flush();

				// Set the header area
				headerArea = Store.GetArea(headerPointer);
			} finally {
				Store.Unlock();
			}
		}

		private void ReadStoreHeaders() {
			// Read the fixed header
			var fixedArea = Store.GetArea(-1);

			// Set the header area
			headerArea = Store.GetArea(fixedArea.ReadInt8());

			// Open a stream to the header
			var version = headerArea.ReadInt4();              // version
			if (version != 1)
				throw new IOException("Incorrect version identifier.");

			TableId = headerArea.ReadInt4();                  // table_id
			sequenceId = headerArea.ReadInt8();               // sequence id
			long infoPointer = headerArea.ReadInt8();         // pointer to DataTableInfo
			long indexInfoPointer = headerArea.ReadInt8();    // pointer to DataIndexSetInfo
			indexHeaderOffset = headerArea.ReadInt8();       // pointer to index header
			listHeaderOffset = headerArea.ReadInt8();        // pointer to list header

			// Read the table info
			using (var stream = Store.GetAreaInputStream(infoPointer)) {
				var reader = new BinaryReader(stream, Encoding.Unicode);
				version = reader.ReadInt32();
				if (version != 1)
					throw new IOException("Incorrect TableInfo version identifier.");

				var userTypeResolver = new TypeResolver(Database);
				TableInfo = TableInfo.Deserialize(stream, userTypeResolver);
				TableInfo.Establish(TableId);
			}

			// Read the data index set info
			using (var stream = Store.GetAreaInputStream(indexInfoPointer)) {
				var reader = new BinaryReader(stream, Encoding.Unicode);
				version = reader.ReadInt32();
				if (version != 1)
					throw new IOException("Incorrect IndexSetInfo version identifier.");

				IndexSetInfo = Sql.IndexSetInfo.DeserializeFrom(stream);
			}

			// Read the list header
			recordList.Open(listHeaderOffset);
			firstDeleteChainRecord = recordList.ReadDeleteHead();

			// Init the index store
			indexSetStore = new IndexSetStore(DatabaseContext, Store);
			try {
				indexSetStore.Open(indexHeaderOffset);
			} catch (IOException) {
				// If this failed try writing output a new empty index set.
				// ISSUE: Should this occur here?  This is really an attempt at repairing
				//   the index store.
				indexSetStore = new IndexSetStore(DatabaseContext, Store);
				indexHeaderOffset = indexSetStore.Create();
				indexSetStore.PrepareIndexLists(TableInfo.ColumnCount + 1, 1, 1024);
				headerArea.Position = 32;
				headerArea.WriteInt8(indexHeaderOffset);
				headerArea.Position = 0;
				headerArea.Flush();
			}
		}

		public long GetNextUniqueId() {
			lock (recordList) {
				long v = sequenceId;
				++sequenceId;
				if (HasShutdown)
					throw new Exception("IO operation while shutting down.");

				try {
					try {
						Store.Lock();
						headerArea.Position = 4 + 4;
						headerArea.WriteInt8(sequenceId);
						headerArea.Flush();
					} finally {
						Store.Unlock();
					}
				} catch (IOException e) {
					throw new InvalidOperationException("IO Error: " + e.Message);
				}

				return v;
			}
		}

		public void SetUniqueId(long value) {
			lock (recordList) {
				sequenceId = value;
				if (HasShutdown)
					throw new Exception("IO operation while shutting down.");

				try {
					try {
						Store.Lock();
						headerArea.Position = 4 + 4;
						headerArea.WriteInt8(sequenceId);
						headerArea.Flush();
					} finally {
						Store.Unlock();
					}
				} catch (IOException e) {
					throw new InvalidOperationException("IO Error: " + e.Message, e);
				}
			}
		}

		public IMutableTable CreateTableAtCommit(ITransaction transaction) {
			return CreateTableAtCommit(transaction, new TableEventRegistry(this));
		}

		public IMutableTable CreateTableAtCommit(ITransaction transaction, TableEventRegistry registry) {
			return new TransactionTable(transaction, this, registry);
		}

		internal void CommitTransactionChange(int commitId, TableEventRegistry change, IIndexSet indexSet) {
			lock (this) {
				// ASSERT: Can't do this if source is Read only.
				if (IsReadOnly)
					throw new InvalidOperationException("Can't commit transaction journal, table is Read only.");

				change.CommitId = commitId;

				try {
					// Add this registry to the multi version table indices log
					tableIndices.AddRegistry(change);

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
									throw new InvalidOperationException(String.Format("Record {0} of table {1} was not in an uncommitted state!",
										rowIndex, TableName));
								}
							} else if (rowEvent.EventType == TableRowEventType.Remove) {
								// Record commit removed
								var oldType = WriteRecordState(rowIndex, RecordState.CommittedRemoved);

								// Check the record was in an added state before we removed it.
								if (oldType != RecordState.CommittedAdded) {
									WriteRecordState(rowIndex, oldType);
									throw new InvalidOperationException(String.Format("Record {0} of table {1} was not in an added state!", rowIndex,
										TableName));
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

		public RecordState WriteRecordState(int rowNumber, RecordState state) {
			lock (recordList) {
				if (HasShutdown)
					throw new IOException("IO operation while shutting down.");

				// Find the record entry input the block list.
				var blockArea = recordList.GetRecord(rowNumber);
				var pos = blockArea.Position;
				// Get the status.
				var oldStatus = (RecordState) blockArea.ReadInt4();

				// Write the new status
				try {
					Store.Lock();

					blockArea.Position = pos;
					blockArea.WriteInt4((int)state);
					blockArea.Flush();
				} finally {
					Store.Unlock();
				}

				return oldStatus;
			}
		}

		public RecordState ReadRecordState(int rowNumber) {
			lock (recordList) {
				// Find the record entry input the block list.
				var blockArea = recordList.GetRecord(rowNumber);
				// Get the status.
				return (RecordState) blockArea.ReadInt4();
			}
		}

		public bool IsRecordDeleted(int rowNumber) {
			var state = ReadRecordState(rowNumber);
			return state == RecordState.Deleted;
		}

		private void DoHardRowRemove(int rowNumber) {
			lock (this) {
				// Internally delete the row,
				OnDeleteRow(rowNumber);

				// Update stats
				//TODO:
			}
		}

		internal void HardRemoveRow(int rowIndex) {
			lock (this) {
				// ASSERTION: We are not under a root Lock.
				if (IsRootLocked)
					throw new InvalidOperationException("Cannot remove row, table is locked");

				var typeKey = ReadRecordState(rowIndex);
				// Check this record is marked as committed removed.
				if (typeKey != RecordState.CommittedRemoved)
					throw new InvalidOperationException(String.Format("The row {0} is not marked as committed removed", rowIndex));

				DoHardRowRemove(rowIndex);
			}
		}

		internal bool HardCheckAndReclaimRow(int recordIndex) {
			lock (this) {
				// ASSERTION: We are not under a root Lock.
				if (IsRootLocked)
					throw new InvalidOperationException("Assertion failed: Can't remove row, table is under a root Lock.");

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

		private void OnDeleteRow(int rowIndex) {
			lock (recordList) {
				if (HasShutdown)
					throw new IOException("IO operation while VM shutting down.");

				// Find the record entry input the block list.
				var blockArea = recordList.GetRecord(rowIndex);
				var p = blockArea.Position;
				var status = (RecordState) blockArea.ReadInt4();

				// Check it is not already deleted
				if (status == RecordState.Deleted)
					throw new IOException("Record is already marked as deleted.");

				long recordPointer = blockArea.ReadInt8();

				// Update the status record.
				try {
					Store.Lock();

					blockArea.Position = p;
					blockArea.WriteInt4((int)RecordState.Deleted);
					blockArea.WriteInt8(firstDeleteChainRecord);
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

		private void RemoveRowFromCache(int rowIndex) {
			if (CellCaching) {
				var colCount = TableInfo.ColumnCount;
				for (int i = 0; i < colCount; i++) {
					CellCache.Remove(DatabaseContext.DatabaseName(), TableId, rowIndex, i);
				}
			}
		}

		private void ReleaseRowObjects(long recordPointer) {
			// NOTE: Does this need to be optimized?
			IArea recordArea = Store.GetArea(recordPointer);
			recordArea.ReadInt4();  // reserved

			// Look for any blob references input the row
			for (int i = 0; i < ColumnCount; ++i) {
				int ctype = recordArea.ReadInt4();
				int cellOffset = recordArea.ReadInt4();

				if (ctype == 1) {
					// Type 1 is not a large object
				} else if (ctype == 2) {
					var curP = recordArea.Position;
					recordArea.Position = cellOffset + 4 + (ColumnCount * 8);

					int btype = recordArea.ReadInt4();
					recordArea.ReadInt4();    // (reserved)

					if (btype == 0) {
						long blobRefId = recordArea.ReadInt8();

						// Release this reference
						ObjectStore.ReleaseObject(blobRefId);
					}

					// Revert the area pointer
					recordArea.Position = curP;
				} else {
					throw new Exception("Unrecognised type.");
				}
			}
		}

		public ILargeObject CreateLargeObject(long maxSize, bool compressed) {
			return ObjectStore.CreateNewObject(maxSize, compressed);
		}

		public ILargeObject GetLargeObject(ObjectId objectId) {
			return ObjectStore.GetObject(objectId);
		}

		public int AddRow(Row row) {
			int rowNumber;

			lock (this) {
				rowNumber = OnAddRow(row);

			} // lock

			// Update stats
			// TODO:

			// Return the record index of the new data in the table
			return rowNumber;
		}


		private long AddToRecordList(long recordPointer) {
			lock (recordList) {
				if (HasShutdown)
					throw new IOException("IO operation while shutting down.");

				// If there are no free deleted records input the delete chain,
				if (firstDeleteChainRecord == -1)
					// Grow the fixed structure to allow more nodes,
					GrowRecordList();

				// Pull free block from the delete chain and recycle it.
				long recycledRecord = firstDeleteChainRecord;
				var block = recordList.GetRecord(recycledRecord);
				var recPos = block.Position;

				// Status of the recycled block
				var status = (RecordState) block.ReadInt4();
				if (status != RecordState.Deleted)
					throw new InvalidOperationException(String.Format("Record {0} is not deleted. ({1})", recPos, status));

				// The pointer to the next input the chain.
				long nextChain = block.ReadInt8();
				firstDeleteChainRecord = nextChain;

				try {
					Store.Lock();

					// Update the first_delete_chain_record field input the header
					recordList.WriteDeleteHead(firstDeleteChainRecord);

					// Update the block
					block.Position = recPos;
					block.WriteInt4((int)RecordState.Uncommitted);
					block.WriteInt8(recordPointer);
					block.Flush();
				} finally {
					Store.Unlock();
				}

				return recycledRecord;
			}
		}

		private void GrowRecordList() {
			try {
				Store.Lock();

				// Increase the size of the list structure.
				recordList.IncreaseSize();

				// The start record of the new size
				int newBlockNumber = recordList.BlockCount - 1;
				long startIndex = recordList.BlockFirstPosition(newBlockNumber);
				long sizeOfBlock = recordList.BlockNodeCount(newBlockNumber);

				// The IArea object for the new position
				var a = recordList.GetRecord(startIndex);

				// Set the rest of the block as deleted records
				for (long n = 0; n < sizeOfBlock - 1; ++n) {
					a.WriteInt4((int)RecordState.Deleted);
					a.WriteInt8(startIndex + n + 1);
				}

				// The last block is end of delete chain.
				a.WriteInt4((int)RecordState.Deleted);
				a.WriteInt8(firstDeleteChainRecord);
				a.Flush();

				// And set the new delete chain
				firstDeleteChainRecord = startIndex;

				// Set the reserved area
				recordList.WriteDeleteHead(firstDeleteChainRecord);
			} finally {
				Store.Unlock();
			}
		}

		private int OnAddRow(Row row) {
			long rowNumber;
			int intRowNumber;

			// Write the record to the store.
			lock (recordList) {
				long recordPointer = WriteRecord(row);

				// Now add this record into the record block list,
				rowNumber = AddToRecordList(recordPointer);
				intRowNumber = (int)rowNumber;
			}

			// Update the cell cache as appropriate
			if (CellCaching && row.CanBeCached) {
				int rowCells = row.ColumnCount;
				for (int i = 0; i < rowCells; ++i) {
					// Put the row/column/TObject into the cache.
					CellCache.Set(Database.Name, TableId, intRowNumber, i, row.GetValue(i));
				}
			}

			// Return the record index of the new data input the table
			// NOTE: We are casting this from a long to int which means we are limited
			//   to ~2 billion record references.
			return (int)rowNumber;
		}

		private long WriteRecord(Row data) {
			// Calculate how much space this record will use
			int rowCells = data.ColumnCount;

			int[] cellSizes = new int[rowCells];
			int[] cellTypes = new int[rowCells];

			try {
				Store.Lock();

				// Establish a reference to any blobs input the record
				int allRecordsSize = 0;
				for (int i = 0; i < rowCells; ++i) {
					var cell = data.GetValue(i);
					int cellSize;
					int cellType;

					if (cell.Value is IObjectRef) {
						var largeObjectRef = (IObjectRef)cell.Value;

						cellSize = 16;
						cellType = 2;
						if (largeObjectRef != null) {
							// Tell the blob store interface that we've made a static reference
							// to this blob.
							ObjectStore.EstablishObject(largeObjectRef.ObjectId.Id);
						}
					} else {
						cellSize = cell.Size;
						cellType = 1;
					}

					cellSizes[i] = cellSize;
					cellTypes[i] = cellType;
					allRecordsSize += cellSize;
				}

				// Allocate space for the record,
				var area = Store.CreateArea(allRecordsSize + (rowCells * 8) + 4);
				long recordPointer = area.Id;

				// The record output stream
				using (var areaStream = new AreaStream(area)) {
					var writer = new BinaryWriter(areaStream);

					// Write the record header first,
					writer.Write(0);        // reserved for future use
					int cellSkip = 0;
					for (int i = 0; i < rowCells; ++i) {
						writer.Write(cellTypes[i]);
						writer.Write(cellSkip);
						cellSkip += cellSizes[i];
					}

					// Now Write a serialization of the cells themselves,
					for (int i = 0; i < rowCells; ++i) {
						var obj = data.GetValue(i);
						int cellType = cellTypes[i];
						if (cellType == 1) {
							// Regular object
							obj.SerializeValueTo(areaStream, SystemContext);
						} else if (cellType == 2) {
							// This is a binary large object and must be represented as a ref
							// to a blob input the BlobStore.
							var largeObjectRef = (IObjectRef)obj.Value;
							if (largeObjectRef == null) {
								// null value
								writer.Write(1);
								writer.Write(0);                  // Reserved for future use
								writer.Write(-1L);
							} else {
								writer.Write(0);
								writer.Write(0);                  // Reserved for future use
								writer.Write(largeObjectRef.ObjectId.Id);
							}
						} else {
							throw new IOException("Unrecognised cell type.");
						}
					}

					// Flush the output
					writer.Flush();
				}

				// Finish the record
				area.Flush();

				// Return the record
				return recordPointer;
			} finally {
				Store.Unlock();
			}
		}

		public void BuildIndexes() {
			lock (this) {
				var indexSet = CreateIndexSet();

				var indexSetInfo = IndexSetInfo;

				int rowCount = RawRowCount;

				// Master index is always on index position 0
				IIndex masterIndex = indexSet.GetIndex(0);

				// First, update the master index
				for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
					// If this row isn't deleted, set the index information for it,
					if (!IsRecordDeleted(rowIndex)) {
						// First add to master inde
						if (!masterIndex.UniqueInsertSort(rowIndex))
							throw new Exception("Master index entry was duplicated.");
					}
				}

				// Commit the master index
				CommitIndexSet(indexSet);

				// Now go ahead and build each index in this table
				int indexCount = indexSetInfo.IndexCount;
				for (int i = 0; i < indexCount; ++i) {
					BuildIndex(i);
				}
			}
		}

		private void BuildIndex(int indexNumber) {
			lock (this) {
				var indexSet = CreateIndexSet();

				// Master index is always on index position 0
				var masterIndex = indexSet.GetIndex(0);

				// A minimal ITable for constructing the indexes
				var minTable = new MinimalTable(this, masterIndex);

				// Set up schemes for the index,
				var index = CreateColumnIndex(indexSet, minTable, indexNumber);

				// Rebuild the entire index
				int rowCount = RawRowCount;
				for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
					// If this row isn't deleted, set the index information for it,
					if (!IsRecordDeleted(rowIndex))
						index.Insert(rowIndex);
				}

				// Commit the index
				CommitIndexSet(indexSet);
			}
		}

		internal ColumnIndex CreateColumnIndex(IIndexSet indexSet, ITable table, int columnOffset) {
			lock (this) {
				var column = TableInfo[columnOffset];
				if (!column.IsIndexable ||
				    (String.IsNullOrEmpty(column.IndexType) ||
				     column.IndexType.Equals(DefaultIndexTypes.BlindSearch)))
					return new BlindSearchIndex(table, columnOffset);

				var indexI = IndexSetInfo.FindIndexForColumns(new[] {column.ColumnName});
				return CreateIndexAt(indexSet, table, indexI);
			}
		}

		private ColumnIndex CreateIndexAt(IIndexSet indexSet, ITable table, int indexI) {
			lock (this) {
				try {
					// Get the IndexDef object
					var indexInfo = IndexSetInfo.GetIndex(indexI);

					if (indexInfo == null)
						return null;

					string[] cols = indexInfo.ColumnNames;
					var tableInfo = TableInfo;
					if (cols.Length != 1)
						throw new Exception("Multi-column indexes not supported at this time.");

					// If a single column
					var colIndex = tableInfo.IndexOfColumn(cols[0]);

					if (indexInfo.IndexType.Equals(DefaultIndexTypes.InsertSearch)) {
						// Get the index from the index set and set up the new InsertSearch
						// scheme.
						var indexList = indexSet.GetIndex(indexInfo.Offset);
						return new InsertSearchIndex(table, colIndex, indexList);
					}

					// TODO: support metadata from input
					return SystemContext.CreateColumnIndex(indexInfo.IndexType, table, colIndex);
				} catch (Exception ex) {
					throw new InvalidOperationException(
						String.Format("An error occurred while creating a colummn for table {0}", TableName), ex);
				}
			}
		}

		public void CopyFrom(int tableId, TableSource destSource, IIndexSet indexSet) {
			throw new NotImplementedException();
		}

		public void AddLock() {
			lock (this) {
				// TODO: Emit the stat to the system
				++rootLock;
			}
		}

		public void RemoveLock() {
			lock (this) {
				if (!isClosed) {
					// TODO: Emit the event to the system

					if (rootLock == 0)
						throw new InvalidOperationException("Too many root locks removed!");

					--rootLock;

					// If the last Lock is removed, schedule a possible collection.
					if (rootLock == 0)
						CheckForCleanup();
				}
			}
		}

		private void CheckForCleanup() {
			lock (this) {
				GC.Collect(false);
			}
		}

		public Field GetValue(int rowIndex, int columnOffset) {
			// NOTES:
			// This is called *A LOT*.  It's a key part of the 20% of the program
			// that's run 80% of the time.
			// This performs very nicely for rows that are completely contained within
			// 1 sector.  However, rows that contain large cells (eg. a large binary
			// or a large string) and spans many sectors will not be utilizing memory
			// as well as it could.
			// The reason is because all the data for a row is Read from the store even
			// if only 1 cell of the column is requested.  This will have a big
			// impact on column scans and searches.  The cell cache takes some of this
			// performance bottleneck away.
			// However, a better implementation of this method is made difficult by
			// the fact that sector spans can be compressed.  We should perhaps
			// revise the low level data storage so only sectors can be compressed.

			// First check if this is within the cache before we continue.
			Field cell;
			if (CellCaching) {
				if (CellCache.TryGetValue(Database.Name, TableId, rowIndex, columnOffset, out cell))
					return cell;
			}

			// We maintain a cache of byte[] arrays that contain the rows Read input
			// from the file.  If consecutive reads are made to the same row, then
			// this will cause lots of fast cache hits.

			long recordPointer = -1;
			try {
				lock (recordList) {
					// Increment the file hits counter
					//TODO:
					//++sRunFileHits;

					//if (sRunFileHits >= 100) {
					//	// TODO: Register the stats
					//	sRunFileHits = 0;
					//}

					// Get the node for the record
					var listBlock = recordList.GetRecord(rowIndex);
					var status = (RecordState) listBlock.ReadInt4();
					// Check it's not deleted
					if (status == RecordState.Deleted)
						throw new InvalidOperationException(String.Format("Record {0} was deleted: unable to read.", rowIndex));

					// Get the pointer to the record we are reading
					recordPointer = listBlock.ReadInt8();
				}

				// Open a stream to the record
				using (var stream = Store.GetAreaInputStream(recordPointer)) {
					var reader = new BinaryReader(stream);

					stream.Seek(4 + (columnOffset * 8), SeekOrigin.Current);

					int cellType = reader.ReadInt32();
					int cellOffset = reader.ReadInt32();

					int curAt = 8 + 4 + (columnOffset * 8);
					int beAt = 4 + (ColumnCount * 8);
					int skipAmount = (beAt - curAt) + cellOffset;

					stream.Seek(skipAmount, SeekOrigin.Current);

					// Get the TType for this column
					// NOTE: It's possible this call may need optimizing?
					var type = TableInfo[columnOffset].ColumnType;

					Objects.ISqlObject ob;
					if (cellType == 1) {
						// If standard object type
						ob = type.DeserializeObject(stream);
					} else if (cellType == 2) {
						// If reference to a blob input the BlobStore
						int fType = reader.ReadInt32();
						int fReserved = reader.ReadInt32();
						long refId = reader.ReadInt64();

						if (fType == 0) {
							// Resolve the reference
							var objRef = ObjectStore.GetObject(refId);
							ob = type.CreateFromLargeObject(objRef);
						} else if (fType == 1) {
							ob = null;
						} else {
							throw new Exception("Unknown blob type.");
						}
					} else {
						throw new Exception("Unrecognised cell type input data.");
					}

					// Wrap it around a TObject
					cell = new Field(type, ob);

					// And close the reader.
#if PCL
					reader.Dispose();
#else
					reader.Close();
#endif
				}
			} catch (IOException e) {
				throw new Exception(String.Format("Error getting cell at ({0}, {1}) pointer = " + recordPointer + ".", rowIndex,
					columnOffset), e);
			}

			// And WriteByte input the cache and return it.
			if (CellCaching) {
				CellCache.Set(Database.Name, TableId, rowIndex, columnOffset, cell);
			}

			return cell;
		}

		#region MinimalTable

		class MinimalTable : ITable {
			private TableSource source;
			private IIndex masterIndex;

			public MinimalTable(TableSource source, IIndex masterIndex) {
				this.source = source;
				this.masterIndex = masterIndex;
			}

			public IEnumerator<Row> GetEnumerator() {
				// NOTE: Returns iterator across master index before journal entry
				//   changes.
				var iterator = masterIndex.GetEnumerator();
				// Wrap it around a IRowEnumerator object.
				return new RowEnumerator(this, iterator);
			}

			private class RowEnumerator : IEnumerator<Row> {
				private MinimalTable table;
				private IIndexEnumerator<int> enumerator;

				public RowEnumerator(MinimalTable table, IIndexEnumerator<int> enumerator) {
					this.table = table;
					this.enumerator = enumerator;
				}

				~RowEnumerator() {
					Dispose(false);
				}

				private void Dispose(bool disposing) {
					table = null;
					enumerator = null;

				}

				public void Dispose() {
					Dispose(true);
					System.GC.SuppressFinalize(this);
				}

				public bool MoveNext() {
					return enumerator.MoveNext();
				}

				public void Reset() {
					enumerator.Reset();
				}

				public Row Current {
					get { return new Row(table, enumerator.Current); }
				}

				object IEnumerator.Current {
					get { return Current; }
				}
			}

			IEnumerator IEnumerable.GetEnumerator() {
				return GetEnumerator();
			}

			public void Dispose() {
				source = null;
				masterIndex = null;
			}

			public IContext Context {
				get { return source.DatabaseContext; }
			}

			public TableInfo TableInfo {
				get { return source.TableInfo; }
			}

			IObjectInfo IDbObject.ObjectInfo {
				get { return TableInfo; }
			}

			public int RowCount {
				get {
					// NOTE: Returns the number of rows in the master index before journal
					//   entries have been made.
					return masterIndex.Count;
				}
			}

			public Field GetValue(long rowNumber, int columnOffset) {
				return source.GetValue((int)rowNumber, columnOffset);
			}

			public ColumnIndex GetIndex(int columnOffset) {
				throw new NotImplementedException();
			}
		}

		#endregion

		public IEnumerable<TableEventRegistry> FindChangesSinceCmmit(long commitId) {
			lock (this) {
				return tableIndices.FindSinceCommit(commitId);
			}
		}

		public void RollbackTransactionChange(TableEventRegistry registry) {
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
									throw new InvalidOperationException(String.Format("Record {0} was not in an uncommitted state.",
										rowEvent.RowNumber));
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

		public void MergeChanges(long commitId) {
			lock (this) {
				bool allMerged = tableIndices.MergeChanges(commitId);
				// If all journal entries merged then schedule deleted row collection.
				if (allMerged && !IsReadOnly) {
					CheckForCleanup();
				}
			}
		}

		#region TypeResolver

		class TypeResolver : ITypeResolver {
			private readonly IDatabase database;

			public TypeResolver(IDatabase database) {
				this.database = database;
			}

			public SqlType ResolveType(TypeResolveContext resolveContext) {
				using (var session = database.CreateSystemSession()) {
					using (var query = session.CreateQuery()) {
						return query.Context.ResolveType(resolveContext.TypeName, resolveContext.GetMeta());
					}
				}
			}
		}

		#endregion
	}
}
