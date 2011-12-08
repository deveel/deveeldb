// 
//  Copyright 2010-2011  Deveel
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

using Deveel.Data.Caching;
using Deveel.Data.Collections;
using Deveel.Data.Store;

using Deveel.Diagnostics;

namespace Deveel.Data {
	/// <summary>
	/// A master table data source provides facilities for read/writing and
	/// maintaining low level data in a table.
	/// </summary>
	/// <remarks>
	/// It provides primitive table operations such as retrieving a cell from 
	/// a table, accessing the table's <see cref="TableInfo"/>, accessing 
	/// indexes, and providing views of transactional versions of the data.
	/// <para>
	/// Logically, a master table data source contains a dynamic number of rows 
	/// and a fixed number of columns. Each row has an associated state - either
	/// <see cref="RecordState.Deleted"/>, <see cref="RecordState.Uncommitted"/>, 
	/// <see cref="RecordState.CommittedAdded"/> or <see cref="RecordState.CommittedRemoved"/>.
	/// A <see cref="RecordState.Deleted"/> row is a row that can be reused by 
	/// a new row added to the table.
	/// </para>
	/// <para>
	/// When a new row is added to the table, it is marked as <see cref="RecordState.Uncommitted"/>.
	/// It is later tagged as <see cref="RecordState.CommittedAdded"/> when the 
	/// transaction that caused the row addition is committed. If a row commits 
	/// a row removal, the row is tagged as <see cref="RecordState.CommittedRemoved"/> 
	/// and later the row garbage collector marks the row as <see cref="RecordState.Deleted"/> 
	/// when there are no remaining references to the row.
	/// </para>
	/// <para>
	/// A master table also maintains a list of indexes for the table.
	/// </para>
	/// <para>
	/// How the master table logical structure is translated to a form that is
	/// stored persistantly is implementation specific. This allows us flexibility
	/// with different types of storage schemes.
	/// </para>
	/// </remarks>
	public abstract partial class MasterTableDataSource {

		// ---------- System information ----------

		/// <summary>
		/// The global TransactionSystem object that points to the global system
		/// that this table source belongs to.
		/// </summary>
		private readonly TransactionSystem system;

		/// <summary>
		/// The IStoreSystem implementation that represents the data persistence layer.
		/// </summary>
		private readonly IStoreSystem storeSystem;

		// ---------- State information ----------

		/// <summary>
		/// An integer that uniquely identifies this data source within the conglomerate.
		/// </summary>
		private int tableId;

		/// <summary>
		/// True if this table source is closed.
		/// </summary>
		private bool isClosed;

		// ---------- Root locking ----------

		/// <summary>
		/// The number of root locks this table data source has on it.
		/// </summary>
		/// <remarks>
		/// While a MasterTableDataSource has at least 1 root lock, it may not
		/// reclaim deleted space in the data store.  A root lock means that data
		/// is still being pointed to in this file (even possibly committed deleted
		/// data).
		/// </remarks>
		private int rootLock;

		// ---------- Persistant data ----------

		/// <summary>
		/// A DataTableDef object that describes the table topology.  This includes
		/// the name and columns of the table.
		/// </summary>
		private DataTableDef tableInfo;

		/// <summary>
		/// A DataIndexSetDef object that describes the indexes on the table.
		/// </summary>
		private DataIndexSetDef indexInfo;

		/// <summary>
		/// A cached TableName for this data source.
		/// </summary>
		private TableName cachedTableName;

		/// <summary>
		/// A multi-version representation of the table indices kept for this table
		/// including the row list and the scheme indices.  This contains the
		/// transaction journals.
		/// </summary>
		private MultiVersionTableIndices tableIndices;

		// ---------- Cached information ----------

		/// <summary>
		/// Set to false to disable cell caching.
		/// </summary>
		private readonly bool dataCellCaching = true;

		/// <summary>
		/// A reference to the DataCellCache object.
		/// </summary>
		private readonly DataCellCache cache;


		/// <summary>
		/// Manages scanning and deleting of rows marked as deleted within this
		/// data source.
		/// </summary>
		private readonly MasterTableGC gc;

		/// <summary>
		/// An abstracted reference to a BlobStore for managing blob 
		/// references and blob data.
		/// </summary>
		private readonly IBlobStore blobStore;

		// ---------- Stat keys ----------

		// The keys we use for Database.Stats for information for this table.
		private string rootLockKey;
		private string totalHitsKey;
		private string fileHitsKey;
		private string deleteHitsKey;
		private string insertHitsKey;

		/// <summary>
		/// Constructs the <see cref="MasterTableDataSource"/>.
		/// </summary>
		/// <param name="system"></param>
		/// <param name="storeSystem"></param>
		/// <param name="blobStore"></param>
		protected MasterTableDataSource(TransactionSystem system, IStoreSystem storeSystem, IBlobStore blobStore) {
			this.system = system;
			this.storeSystem = storeSystem;
			this.blobStore = blobStore;
			gc = new MasterTableGC(this);
			cache = system.DataCellCache;
			isClosed = true;

			if (dataCellCaching) {
				dataCellCaching = (cache != null);
			}
		}

		/// <summary>
		/// Returns the TransactionSystem for this table.
		/// </summary>
		public TransactionSystem System {
			get { return system; }
		}

		/// <summary>
		/// Returns the IDebugLogger object that can be used to log debug messages.
		/// </summary>
		protected IDebugLogger Debug {
			get { return System.Debug; }
		}

		internal IDebugLogger InternalDebug {
			get { return Debug; }
		}

		/// <summary>
		/// Returns the TableName of this table source.
		/// </summary>
		public TableName TableName {
			get { return TableInfo.TableName; }
		}

		/// <summary>
		/// Returns the name of this table source.
		/// </summary>
		public string Name {
			get { return TableInfo.Name; }
		}

		/// <summary>
		/// Returns the schema name of this table source.
		/// </summary>
		public string Schema {
			get { return TableInfo.Schema; }
		}

		/// <summary>
		/// Returns a cached TableName for this data source.
		/// </summary>
		internal TableName CachedTableName {
			get {
				lock (this) {
					return cachedTableName ?? (cachedTableName = TableName);
				}
			}
		}

		/// <summary>
		/// Returns table_id - the unique identifier for this data source.
		/// </summary>
		public int TableId {
			get { return tableId; }
			protected set { tableId = value; }
		}

		/// <summary>
		/// Returns the DataTableDef object that represents the topology of this
		/// table data source (name, columns, etc).
		/// </summary>
		/// <remarks>
		/// This information can't be changed during the lifetime of a data source.
		/// </remarks>
		public DataTableDef TableInfo {
			get { return tableInfo; }
			protected set { tableInfo = value; }
		}

		/// <summary>
		/// Returns the <see cref="DataIndexSetDef"/> object that represents 
		/// the indexes on this table.
		/// </summary>
		public DataIndexSetDef IndexSetInfo {
			get { return indexInfo; }
			protected set { indexInfo = value; }
		}

		/// <summary>
		/// Returns a string that uniquely identifies this table within the
		/// conglomerate context.
		/// </summary>
		/// <remarks>
		/// For example, the filename of the table.  This string can be used 
		/// to open and initialize the table also.
		/// </remarks>
		public abstract string SourceIdentity { get; set; }

		/// <summary>
		/// Returns the raw count or rows in the table, including uncommited,
		/// committed and deleted rows.
		/// </summary>
		/// <remarks>
		/// This is basically the maximum number of rows we can iterate through.
		/// </remarks>
		public abstract int RawRowCount { get; }

		/// <summary>
		/// Atomically returns the current 'unique_id' value for this table.
		/// </summary>
		public abstract long CurrentUniqueId { get; }

		/// <summary>
		/// Atomically returns the next 'unique_id' value from this table.
		/// </summary>
		public abstract long GetNextUniqueId();

		/// <summary>
		/// Returns true if a compact table is necessary.
		/// </summary>
		/// <remarks>
		/// By default, we return true however it is recommended this method 
		/// is overwritten and the table tested.
		/// </remarks>
		public virtual bool Compact {
			get { return true; }
		}

		/// <summary>
		/// Returns true if this table source is closed.
		/// </summary>
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

		/// <summary>
		/// Returns true if the source is read only.
		/// </summary>
		public bool IsReadOnly {
			get { return system.ReadOnlyAccess; }
		}

		/// <summary>
		/// Returns the IStoreSystem object used to manage stores in the 
		/// persistence system.
		/// </summary>
		protected IStoreSystem StoreSystem {
			get { return storeSystem; }
		}

		/// <summary>
		/// Returns true if the table is currently under a root lock (has 1 
		/// or more root locks on it).
		/// </summary>
		public bool IsRootLocked {
			get {
				lock (this) {
					return rootLock > 0;
				}
			}
		}

		/// <summary>
		/// Returns true if this table has any journal modifications that have 
		/// not yet been incorporated into master index.
		/// </summary>
		internal bool HasTransactionChangesPending {
			get {
				lock (this) {
					return tableIndices.HasTransactionChangesPending;
				}
			}
		}

		/// <summary>
		/// Manages scanning and deleting of rows marked as deleted within this
		/// data source.
		/// </summary>
		protected MasterTableGC TableGC {
			get { return gc; }
		}

		/// <summary>
		/// An abstracted reference to a BlobStore for managing blob 
		/// references and blob data.
		/// </summary>
		protected IBlobStore BlobStore {
			get { return blobStore; }
		}

		protected string RootLockKey {
			get { return rootLockKey; }
		}

		protected string FileHitsKey {
			get { return fileHitsKey; }
		}

		protected string DeleteHitsKey {
			get { return deleteHitsKey; }
		}

		protected string TotalHitsKey {
			get { return totalHitsKey; }
		}

		protected string InsertHitsKey {
			get { return insertHitsKey; }
		}

		/// <summary>
		/// A reference to the DataCellCache object.
		/// </summary>
		protected DataCellCache Cache {
			get { return cache; }
		}

		/// <summary>
		/// Gets whether the cell value caching is enabled.
		/// </summary>
		protected bool CellCaching {
			get { return dataCellCaching; }
		}

		/// <summary>
		/// Opens an existing master table.
		/// </summary>
		/// <remarks>
		/// This will set up the internal state of this object with the 
		/// data read input.
		/// </remarks>
		public void Open() {
			bool needsCheck = OpenTable();

			// Create table indices
			tableIndices = new MultiVersionTableIndices(System, this);

			// Load internal state
			LoadInternal();

			if (needsCheck) {
				// Do an opening scan of the table.  Any records that are uncommited
				// must be marked as deleted.
				DoOpeningScan();
			}
		}

		/// <summary>
		/// Create this master table object.
		/// </summary>
		/// <param name="id"></param>
		/// <param name="info"></param>
		/// <remarks>
		/// This will initialise the various objects and result input a new empty 
		/// master table to store data input.
		/// </remarks>
		public void Create(int id, DataTableDef info) {
			// Set the data table def object
			SetTableInfo(info);

			// Load internal state
			LoadInternal();

			// Set up internal state of this object
			TableId = id;

			CreateTable();
		}

		// ---------- Abstract methods ----------

		/// <summary>
		/// Concretely creates the table, setting up the underlying state.
		/// </summary>
		protected abstract void CreateTable();

		/// <summary>
		/// Opens an existing table from the underlying implementation.
		/// </summary>
		/// <returns>
		/// Returns <b>true</b> if the table needs to be scanned for
		/// errors, or <b>false</b> if the open process can continue.
		/// </returns>
		protected abstract bool OpenTable();

		/// <summary>
		/// Sets the record type for the given record in the table and returns 
		/// the previous state of the record.
		/// </summary>
		/// <param name="rowIndex"></param>
		/// <param name="rowState"></param>
		/// <remarks>
		/// This is used to change the state of a row in the table.
		/// </remarks>
		/// <returns></returns>
		public abstract int WriteRecordType(int rowIndex, int rowState);

		/// <summary>
		/// Reads the record state for the given record in the table.
		/// </summary>
		/// <param name="rowIndex"></param>
		/// <returns></returns>
		public abstract int ReadRecordType(int rowIndex);

		/// <summary>
		/// Returns true if the record with the given index is deleted from the 
		/// table.
		/// </summary>
		/// <param name="rowIndex"></param>
		/// <remarks>
		/// A deleted row can not be read.
		/// </remarks>
		/// <returns></returns>
		protected abstract bool IsRecordDeleted(int rowIndex);

		/// <summary>
		/// Removes the row at the given index so that any resources associated
		/// with the row may be immediately available to be recycled.
		/// </summary>
		/// <param name="rowIndex"></param>
		protected abstract void OnDeleteRow(int rowIndex);

		/// <summary>
		/// Adds a new row to this table and returns an index that is used to
		/// reference this row by the <see cref="GetCellContents"/> method.
		/// </summary>
		/// <param name="data"></param>
		/// <remarks>
		/// Note that this method will not effect the master index or column 
		/// schemes. This is a low level mechanism for adding unreferenced 
		/// data into a conglomerate. The data is referenced by committing the 
		/// change where it eventually migrates into the master index and schemes.
		/// </remarks>
		/// <returns></returns>
		protected abstract int OnAddRow(DataRow data);

		/// <summary>
		/// Returns the cell contents of the given cell in the table.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="row"></param>
		/// <remarks>
		/// It is the responsibility of the implemented method to perform 
		/// caching as it deems fit. Some representations may not require 
		/// such extensive caching as others.
		/// </remarks>
		/// <returns></returns>
		protected abstract TObject OnGetCellContents(int column, int row);

		/// <summary>
		/// Sets the unique id for this store.
		/// </summary>
		/// <param name="value"></param>
		/// <remarks>
		/// This must only be used under extraordinary circumstances, such as 
		/// restoring from a backup, or converting from one file to another.
		/// </remarks>
		public abstract void SetUniqueID(long value);

		/// <summary>
		/// Disposes of all in-memory resources associated with this table and
		/// invalidates this object.
		/// </summary>
		/// <param name="pendingDrop">If true the table is to be disposed 
		/// pending a call to <see cref="Drop"/> and any persistant resources 
		/// that are allocated may be freed.</param>
		public abstract void Dispose(bool pendingDrop);

		/// <summary>
		/// Disposes and drops this table.
		/// </summary>
		/// <remarks>
		/// If the drop failed, it should be retried at a later time.
		/// </remarks>
		/// <returns>
		/// If the dispose failed for any reason, it returns <b>false</b>, 
		/// otherwise <b>true</b>.
		/// </returns>
		public abstract bool Drop();

		/// <summary>
		/// Called by the 'shutdown hook' on the conglomerate.
		/// </summary>
		/// <remarks>
		/// This method should block until the table can by write into a safe 
		/// mode and then prevent any further access to the object after it 
		/// returns.  It must operate very quickly.
		/// </remarks>
		public abstract void ShutdownHookCleanup();

		/// <summary>
		/// Creates a new master table data source that is a copy of the 
		/// given <see cref="MasterTableDataSource"/> object.
		/// </summary>
		/// <param name="tableId">The table id to given the new table.</param>
		/// <param name="srcMasterTable">The table to copy.</param>
		/// <param name="indexSet">The view of the table to be copied.</param>
		public abstract void CopyFrom(int tableId, MasterTableDataSource srcMasterTable, IIndexSet indexSet);

		/// <summary>
		/// Creates a minimal <see cref="ITableDataSource"/> implementation 
		/// that represents this <see cref="MasterTableDataSource"/>.
		/// </summary>
		/// <param name="masterIndex"></param>
		/// <remarks>
		/// The implementation returned does not implement the 
		/// <see cref="ITableDataSource.GetColumnScheme"/> method.
		/// </remarks>
		/// <returns></returns>
		protected ITableDataSource GetMinimalTableDataSource(IIntegerList masterIndex) {
			// Make a ITableDataSource that represents the master table over this
			// index.
			return new MinimalTableDataSource(this, masterIndex);
		}

		/// <summary>
		/// Sets up the DataTableDef.
		/// </summary>
		/// <param name="info"></param>
		/// <remarks>
		/// This would typically only ever be called from the <i>create</i>
		/// method.
		/// </remarks>
		protected void SetTableInfo(DataTableDef info) {
			lock (this) {
				// Check table_id isn't too large.
				if ((tableId & 0x0F0000000) != 0)
					throw new ApplicationException("'table_id' exceeds maximum possible keys.");

				tableInfo = info;

				// Create table indices
				tableIndices = new MultiVersionTableIndices(System, this);

				// Setup the DataIndexSetDef
				SetIndexSetInfo();
			}
		}

		/// <summary>
		/// Loads the internal variables.
		/// </summary>
		private void LoadInternal() {
			lock (this) {
				// Set up the stat keys.
				string tableName = tableInfo.Name;
				string schemaName = tableInfo.Schema;
				string n = tableName;
				if (schemaName.Length > 0) {
					n = schemaName + "." + tableName;
				}
				rootLockKey = "MasterTableDataSource.RootLocks." + n;
				totalHitsKey = "MasterTableDataSource.Hits.Total." + n;
				fileHitsKey = "MasterTableDataSource.Hits.File." + n;
				deleteHitsKey = "MasterTableDataSource.Hits.Delete." + n;
				insertHitsKey = "MasterTableDataSource.Hits.Insert." + n;

				isClosed = false;
			}
		}

		/// <summary>
		/// Adds a new row to this table and returns an index that is used to
		/// reference this row by the <see cref="GetCellContents"/> method.
		/// </summary>
		/// <param name="data"></param>
		/// <remarks>
		/// Note that this method will not effect the master index or column 
		/// schemes. This is a low level mechanism for adding unreferenced data 
		/// into a conglomerate. The data is referenced by committing the 
		/// change where it eventually migrates into the master index and schemes.
		/// </remarks>
		/// <returns></returns>
		public int AddRow(DataRow data) {
			int rowNumber;

			lock (this) {
				rowNumber = OnAddRow(data);

			} // lock

			// Update stats
			System.Stats.Increment(insertHitsKey);

			// Return the record index of the new data in the table
			return rowNumber;
		}

		/// <summary>
		/// Actually deletes the row from the table.
		/// </summary>
		/// <param name="rowIndex"></param>
		/// <remarks>
		/// This is a permanent removal of the row from the table. After this 
		/// method is called, the row can not be retrieved again. This is 
		/// generally only used by the row garbage collector.
		/// <para>
		/// There is no checking in this method.
		/// </para>
		/// </remarks>
		private void DoHardRowRemove(int rowIndex) {
			lock (this) {
				// Internally delete the row,
				OnDeleteRow(rowIndex);

				// Update stats
				system.Stats.Increment(deleteHitsKey);
			}
		}

		/// <summary>
		/// Permanently removes a row from this table.
		/// </summary>
		/// <param name="record_index"></param>
		/// <remarks>
		/// This must only be used when it is determined that a transaction 
		/// does not reference this row, and that an open result set does not 
		/// reference this row. This will remove the row permanently from the 
		/// underlying file representation. Calls to <see cref="GetCellContents"/>
		/// where row is deleted will be undefined after this method is called.
		/// <para>
		/// Note that the removed row must not be contained within the master 
		/// index, or be referenced by the index schemes, or be referenced in 
		/// the transaction modification list.
		/// </para>
		/// </remarks>
		internal void HardRemoveRow(int record_index) {
			lock (this) {
				// ASSERTION: We are not under a root Lock.
				if (IsRootLocked)
					throw new ApplicationException("Assertion failed: " +
					                               "Can't remove row, table is under a root Lock.");

				int typeKey = ReadRecordType(record_index);
				// Check this record is marked as committed removed.
				if ((typeKey & 0x0F0) != 0x020)
					throw new ApplicationException("Row isn't marked as committed removed: " + record_index);

				DoHardRowRemove(record_index);
			}
		}

		/// <summary>
		/// Checks the given record index, and if it's possible to reclaim it 
		/// then it does so here.
		/// </summary>
		/// <param name="recordIndex"></param>
		/// <remarks>
		/// Rows are only removed if they are marked as committed removed.
		/// </remarks>
		/// <returns></returns>
		internal bool HardCheckAndReclaimRow(int recordIndex) {
			lock(this) {
				// ASSERTION: We are not under a root Lock.
				if (IsRootLocked)
					throw new ApplicationException("Assertion failed: Can't remove row, table is under a root Lock.");

				// Row already deleted?
				if (IsRecordDeleted(recordIndex)) 
					return false;

				int typeKey = ReadRecordType(recordIndex);
				// Check this record is marked as committed removed.
				if ((typeKey & 0x0F0) != 0x020)
					return false;

				DoHardRowRemove(recordIndex);
				return true;
			}
		}

		/// <summary>
		/// Returns the record type of the given record index.
		/// </summary>
		/// <param name="recordIndex"></param>
		/// <returns>
		/// Returns a type that is compatible with RawDiagnosticTable record 
		/// type.
		/// </returns>
		internal RecordState RecordTypeInfo(int recordIndex) {
			lock (this) {
				if (IsRecordDeleted(recordIndex))
					return RecordState.Deleted;

				int typeKey = ReadRecordType(recordIndex) & 0x0F0;
				if (typeKey == 0)
					return RecordState.Uncommitted;
				if (typeKey == 0x010)
					return RecordState.CommittedAdded;
				if (typeKey == 0x020)
					return RecordState.CommittedRemoved;
				return RecordState.Error;

			}
		}


		/// <summary>
		/// Returns the cell contents of the given cell in the table.
		/// </summary>
		/// <param name="column"></param>
		/// <param name="row"></param>
		/// <remarks>
		/// This will look up the cell in the file if it can't be found in 
		/// the cell cache. This method is undefined if row has been removed 
		/// or was not returned by the <see cref="AddRow"/> method.
		/// </remarks>
		/// <returns></returns>
		public TObject GetCellContents(int column, int row) {
			if (row < 0)
				throw new ApplicationException("'row' is < 0");

			return OnGetCellContents(column, row);
		}

		/// <summary>
		/// Grabs a root lock on this table.
		/// </summary>
		/// <remarks>
		/// While a <see cref="MasterTableDataSource"/> has at least 1 root 
		/// lock, it may not reclaim deleted space in the data store. A root 
		/// lock means that data is still being pointed to in this file (even 
		/// possibly committed deleted data).
		/// </remarks>
		public void AddRootLock() {
			lock (this) {
				system.Stats.Increment(rootLockKey);
				++rootLock;
			}
		}

		/// <summary>
		/// Removes a root lock from this table.
		/// </summary>
		/// <remarks>
		/// While a <see cref="MasterTableDataSource"/> has at least 1 root 
		/// lock, it may not reclaim deleted space in the data store. A root 
		/// lock means that data is still being pointed to in this file (even 
		/// possibly committed deleted data).
		/// </remarks>
		public void RemoveRootLock() {
			lock (this) {
				if (!isClosed) {
					system.Stats.Decrement(rootLockKey);
					if (rootLock == 0)
						throw new ApplicationException("Too many root locks removed!");

					--rootLock;

					// If the last Lock is removed, schedule a possible collection.
					if (rootLock == 0)
						CheckForCleanup();
				}
			}
		}

		/// <summary>
		/// Clears all root locks on the table.
		/// </summary>
		/// <remarks>
		/// Should only be used during cleanup of the table and will by 
		/// definition invalidate the table.
		/// </remarks>
		protected void ClearAllRootLocks() {
			lock (this) {
				rootLock = 0;
			}
		}

		private class MinimalTableDataSource : ITableDataSource {
			private readonly MasterTableDataSource mtds;
			private readonly IIntegerList masterIndex;

			public MinimalTableDataSource(MasterTableDataSource mtds, IIntegerList masterIndex) {
				this.mtds = mtds;
				this.masterIndex = masterIndex;
			}

			public TransactionSystem System {
				get { return mtds.system; }
			}

			public DataTableDef TableInfo {
				get { return mtds.TableInfo; }
			}

			public int RowCount {
				get {
					// NOTE: Returns the number of rows in the master index before journal
					//   entries have been made.
					return masterIndex.Count;
				}
			}

			public IRowEnumerator GetRowEnumerator() {
				// NOTE: Returns iterator across master index before journal entry
				//   changes.
				// Get an iterator across the row list.
				IIntegerIterator iterator = masterIndex.GetIterator();
				// Wrap it around a IRowEnumerator object.
				return new RowEnumerator(iterator);
			}

			public SelectableScheme GetColumnScheme(int column) {
				throw new NotImplementedException();
			}

			public TObject GetCellContents(int column, int row) {
				return mtds.GetCellContents(column, row);
			}

			private class RowEnumerator : IRowEnumerator {
				public RowEnumerator(IIntegerIterator iterator) {
					this.iterator = iterator;
				}

				private readonly IIntegerIterator iterator;

				public bool MoveNext() {
					return iterator.MoveNext();
				}

				public void Reset() {
				}

				public object Current {
					get { return RowIndex; }
				}

				public int RowIndex {
					get { return iterator.Next; }
				}
			}
		}
	}
}