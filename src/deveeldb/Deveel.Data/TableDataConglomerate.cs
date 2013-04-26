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
using System.ComponentModel;
using System.IO;
using System.Text;

using Deveel.Data.Store;

using Deveel.Diagnostics;

namespace Deveel.Data {
	/// <summary>
	/// A conglomerate of data that represents the contents of all tables in a
	/// complete database.
	/// </summary>
	/// <remarks>
	/// This object handles all data persistance management (storage, retrieval, 
	/// removal) issues. It is a transactional manager for both data and indices 
	/// in the database.
	/// </remarks>
	public sealed partial class TableDataConglomerate : IDisposable {
		/// <summary>
		/// The postfix on the name of the state file for the database store name.
		/// </summary>
		public const String StatePost = "_sf";

		// ---------- The standard constraint/schema tables ----------

		/// <summary>
		/// The name of the system schema where persistant conglomerate 
		/// state is stored.
		/// </summary>
		public const String SystemSchema = "SYSTEM";

		/**
		 * The schema info table.
		 */
		///<summary>
		///</summary>
		public static readonly TableName SchemaInfoTable = new TableName(SystemSchema, "schema_info");
		///<summary>
		///</summary>
		public static readonly TableName PersistentVarTable = new TableName(SystemSchema, "database_vars");
		///<summary>
		///</summary>
		public static readonly TableName ForeignColsTable = new TableName(SystemSchema, "foreign_columns");
		///<summary>
		///</summary>
		public static readonly TableName UniqueColsTable = new TableName(SystemSchema, "unique_columns");
		///<summary>
		///</summary>
		public static readonly TableName PrimaryColsTable = new TableName(SystemSchema, "primary_columns");
		///<summary>
		///</summary>
		public static readonly TableName CheckInfoTable = new TableName(SystemSchema, "check_info");
		///<summary>
		///</summary>
		public static readonly TableName UniqueInfoTable = new TableName(SystemSchema, "unique_info");
		///<summary>
		///</summary>
		public static readonly TableName ForeignInfoTable = new TableName(SystemSchema, "fkey_info");
		///<summary>
		///</summary>
		public static readonly TableName PrimaryInfoTable = new TableName(SystemSchema, "pkey_info");
		///<summary>
		///</summary>
		public static readonly TableName SysSequenceInfo = new TableName(SystemSchema, "sequence_info");
		///<summary>
		///</summary>
		public static readonly TableName SysSequence = new TableName(SystemSchema, "sequence");
		///<summary>
		///</summary>
		public static readonly TableName UdtTable = new TableName(SystemSchema, "udt_info");
		///<summary>
		///</summary>
		public static readonly TableName UdtMembersTable = new TableName(SystemSchema, "udt_member");

		/// <summary>
		/// The TransactionSystem that this Conglomerate is a child of.
		/// </summary>
		private readonly TransactionSystem system;

		/// <summary>
		/// The <see cref="IStoreSystem"/> object used by this conglomerate to 
		/// store the underlying representation.
		/// </summary>
		private readonly IStoreSystem storeSystem;

		/// <summary>
		/// The name given to this conglomerate.
		/// </summary>
		private readonly string name;

		/// <summary>
		/// The actual store that backs the state store.
		/// </summary>
		private IStore actStateStore;

		/// <summary>
		/// A store for the conglomerate state container.
		/// </summary>
		/// <remarks>
		/// This file stores information persistantly about the state of this object.
		/// </remarks>
		private StateStore stateStore;

		/// <summary>
		/// The storage that backs temporary tables.
		/// </summary>
		private readonly IStoreSystem tempStoreSystem;

		/// <summary>
		/// The current commit id for committed transactions.
		/// </summary>
		/// <remarks>
		/// Whenever transactional changes are committed to the conglomerate, this id 
		/// is incremented.
		/// </remarks>
		private long commitId;


		/// <summary>
		/// The list of all tables that are currently open in this conglomerate.
		/// </summary>
		/// <remarks>
		/// This includes tables that are not committed.
		/// </remarks>
		private List<MasterTableDataSource> tableList;

		/// <summary>
		/// The actual <see cref="IStore"/> implementation that maintains the <see cref="IBlobStore"/> 
		/// information for this conglomerate (if there is one).
		/// </summary>
		private IStore actBlobStore;

		/// <summary>
		/// The <see cref="IBlobStore"/> object for this conglomerate.
		/// </summary>
		private BlobStore blobStore;

		/// <summary>
		/// The <see cref="Data.SequenceManager"/> object for this conglomerate.
		/// </summary>
		private readonly SequenceManager sequenceManager;

		/// <summary>
		/// The <see cref="Data.UDTManager"/> object for this conglomerate.
		/// </summary>
		private readonly UDTManager udtManager;


		// ---------- Locks ----------

		/// <summary>
		/// This Lock is obtained when we go to commit a change to the table.
		/// </summary>
		/// <remarks>
		/// Grabbing this lock ensures that no other commits can occur at the same
		/// time on this conglomerate.
		/// </remarks>
		private readonly Object CommitLock = new Object();



		internal TableDataConglomerate(TransactionSystem system, string name, IStoreSystem storeSystem) {
			this.system = system;
			this.name = name;
			this.storeSystem = storeSystem;

			// temporary tables live in memory
			tempStoreSystem = new V1HeapStoreSystem();

			openTransactions = new OpenTransactionList(system);
			modificationEvents = new EventHandlerList();
			namespaceJournalList = new List<NameSpaceJournal>();

			sequenceManager = new SequenceManager(this);
			udtManager = new UDTManager(this);

		}

		/// <summary>
		/// Returns the <see cref="TransactionSystem"/> that this conglomerate is part of.
		/// </summary>
		public TransactionSystem System {
			get { return system; }
		}

		/// <summary>
		/// Returns the IStoreSystem used by this conglomerate to manage the persistent 
		/// state of the database.
		/// </summary>
		internal IStoreSystem StoreSystem {
			get { return storeSystem; }
		}

		/// <summary>
		/// Returns the SequenceManager object for this conglomerate.
		/// </summary>
		internal SequenceManager SequenceManager {
			get { return sequenceManager; }
		}

		internal UDTManager UDTManager {
			get { return udtManager; }
		}


		/// <summary>
		/// Returns the BlobStore for this conglomerate.
		/// </summary>
		internal BlobStore BlobStore {
			get { return blobStore; }
		}

		/// <summary>
		/// Gets or sets the name given to this conglomerate.
		/// </summary>
		public string Name {
			get { return name; }
		}

		internal Logger Logger {
			get { return System.Logger; }
		}

		// ---------- Conglomerate state methods ----------

		/// <summary>
		/// Returns true if the system is in read-only mode.
		/// </summary>
		private bool IsReadOnly {
			get { return system.ReadOnlyAccess; }
		}

		/// <summary>
		/// Returns true if the conglomerate is closed.
		/// </summary>
		public bool IsClosed {
			get {
				lock (CommitLock) {
					return tableList == null;
				}
			}
		}

		/// <summary>
		/// Reads in the list of committed dropped tables on this conglomerate.
		/// </summary>
		/// <remarks>
		/// This should only be called during an 'open' like method. This method 
		/// fills the 'committed_dropped' and 'table_list' lists with the tables 
		/// in this conglomerate.
		/// </remarks>
		private void ReadDroppedTables() {

			// The list of all dropped tables from the state file
			IEnumerable<StateStore.StateResource> tables = stateStore.GetDeleteList();
			// For each visible table
			foreach (StateStore.StateResource resource in tables) {
				int masterTableId = (int)resource.TableId;
				string fileName = resource.Name;

				// Parse the file name string and determine the table type.
				int tableType = 1;
				if (fileName.StartsWith(":")) {
					if (fileName[1] == '1')
						throw new NotSupportedException();

					if (fileName[1] != '2')
						throw new Exception("Table type is not known.");

					tableType = 2;
					fileName = fileName.Substring(2);
				}

				// Load the master table from the resource information
				MasterTableDataSource master = LoadMasterTable(masterTableId, fileName, tableType);

				// File wasn't found so remove from the delete resources
				if (master == null) {
					stateStore.RemoveDeleteResource(resource.Name);
				} else {
					if (!(master is V2MasterTableDataSource))
						throw new ApplicationException("Unknown master table type: " + master.GetType());

					V2MasterTableDataSource v2Master = (V2MasterTableDataSource) master;
					v2Master.SourceIdentity = fileName;
					v2Master.Open();

					// Add the table to the table list
					tableList.Add(master);
				}
			}

			// Commit any changes to the state store
			stateStore.Commit();
		}

		/// <summary>
		/// Marks the given table id as committed dropped.
		/// </summary>
		/// <param name="tableId"></param>
		private void MarkAsCommittedDropped(int tableId) {
			MasterTableDataSource masterTable = GetMasterTable(tableId);
			stateStore.AddDeleteResource(new StateStore.StateResource(tableId, CreateEncodedTableFile(masterTable)));
		}

		/// <summary>
		/// Loads the master table given the table_id and the name of the table
		/// resource in the database path.
		/// </summary>
		/// <param name="tableId"></param>
		/// <param name="tableStr"></param>
		/// <param name="tableType"></param>
		/// <remarks>
		/// The <paramref name="tableStr"/> string is a specially formatted string that we parse to 
		/// determine the file structure of the table.
		/// </remarks>
		/// <returns></returns>
		private MasterTableDataSource LoadMasterTable(int tableId, string tableStr, int tableType) {
			// Open the table
			if (tableType == 1)
				throw new NotSupportedException();
			if (tableType == 2) {
				V2MasterTableDataSource master = new V2MasterTableDataSource(System, StoreSystem, blobStore);
				if (master.Exists(tableStr))
					return master;
			}

			// If not exists, then generate an error message
			Logger.Error(this, "Couldn't find table source - resource name: " + tableStr + " table_id: " + tableId);

			return null;
		}

		/// <summary>
		/// Returns a string that is an encoded table file name.
		/// </summary>
		/// <param name="table"></param>
		/// <remarks>
		/// An encoded table file name includes information about the table 
		/// type with the name of the table.
		/// </remarks>
		/// <example>
		/// <c>:2ThisTable</c> represents a <see cref="V2MasterTableDataSource"/> 
		/// table with file name <c>ThisTable</c>.
		/// </example>
		/// <returns></returns>
		private static String CreateEncodedTableFile(MasterTableDataSource table) {
			char type;
			if (table is V2MasterTableDataSource) {
				type = '2';
			} else {
				throw new Exception("Unrecognised MasterTableDataSource class.");
			}
			StringBuilder buf = new StringBuilder();
			buf.Append(':');
			buf.Append(type);
			buf.Append(table.SourceIdentity);
			return buf.ToString();
		}

		/// <summary>
		/// Reads in the list of committed tables in this conglomerate.
		/// </summary>
		/// <remarks>
		/// This should only be called during an <see cref="Open"/> like method.
		/// This method fills the 'committed_tables' and 'table_list' lists 
		/// with the tables in this conglomerate.
		/// </remarks>
		private void ReadVisibleTables() {
			// The list of all visible tables from the state file
			IEnumerable<StateStore.StateResource> tables = stateStore.GetVisibleList();
			// For each visible table
			foreach (StateStore.StateResource resource in tables) {
				int masterTableId = (int)resource.TableId;
				string fileName = resource.Name;

				// Parse the file name string and determine the table type.
				int table_type = 1;
				if (fileName.StartsWith(":")) {
					if (fileName[1] == '1')
						throw new NotSupportedException();
					if (fileName[1] != '2')
						throw new Exception("Table type is not known.");
					table_type = 2;
					fileName = fileName.Substring(2);
				}

				// Load the master table from the resource information
				MasterTableDataSource master = LoadMasterTable(masterTableId, fileName, table_type);

				if (master == null)
					throw new ApplicationException("Table file for " + fileName + " was not found.");

				if (!(master is V2MasterTableDataSource))
					throw new ApplicationException("Unknown master table type: " + master.GetType());

				V2MasterTableDataSource v2Master = (V2MasterTableDataSource) master;
				v2Master.SourceIdentity = fileName;
				v2Master.Open();

				// Add the table to the table list
				tableList.Add(master);

			}
		}

		// ---------- Private methods ----------

		/// <summary>
		/// Returns the next unique table_id value for a new table and updates the
		/// conglomerate state information as appropriate.
		/// </summary>
		/// <returns></returns>
		private int NextUniqueTableID() {
			return stateStore.NextTableId();
		}


		/// <summary>
		/// Sets up the internal state of this object.
		/// </summary>
		private void SetupInternal() {
			commitId = 0;
			tableList = new List<MasterTableDataSource>();
		}

		// ---------- Public methods ----------


		/// <summary>
		/// Opens a conglomerate.
		/// </summary>
		/// <remarks>
		/// Once a conglomerate is open, we may start opening transactions and 
		/// altering the data within it.
		/// </remarks>
		/// <exception cref="IOException">
		/// If the conglomerate does not exist.  
		/// </exception>
		public void Open() {
			if (!Exists())
				throw new IOException("Conglomerate doesn't exists: " + name);

			// Check the file Lock
			if (!IsReadOnly) {
				// Obtain the Lock (generate error if this is not possible)
				StoreSystem.Lock(name);
			}

			// Open the state store
			actStateStore = StoreSystem.OpenStore(name + StatePost);
			stateStore = new StateStore(actStateStore);
			// Get the fixed 64 byte area.
			IArea fixedArea = actStateStore.GetArea(-1);
			long headP = fixedArea.ReadInt8();
			stateStore.Init(headP);

			SetupInternal();

			// Init the conglomerate blob store
			InitializeBlobStore();

			ReadVisibleTables();
			ReadDroppedTables();

			// We possibly have things to clean up if there are deleted columns.
			CleanUpConglomerate();
		}

		/// <summary>
		/// Closes this conglomerate.
		/// </summary>
		/// <remarks>
		/// The conglomerate must be open for it to be closed. When closed, 
		/// any use of this object is undefined.
		/// </remarks>
		public void Close() {
			lock (CommitLock) {

				// We possibly have things to clean up.
				CleanUpConglomerate();

				// Set a check point
				storeSystem.SetCheckPoint();

				// Go through and close all the committed tables.
				foreach (MasterTableDataSource master in tableList) {
					master.Dispose(false);
				}

				stateStore.Commit();
				StoreSystem.CloseStore(actStateStore);

				tableList = null;
			}

			// Unlock the storage system
			StoreSystem.Unlock(name);

			if (blobStore != null)
				StoreSystem.CloseStore(actBlobStore);

			//    removeShutdownHook();
		}

		/// <summary>
		/// Deletes and closes the conglomerate.
		/// </summary>
		/// <remarks>
		/// This will delete all the files in the file system associated with 
		/// this conglomerate, so this method should be used with care.
		/// <para>
		/// <b>Warning</b> Will result in total loss of all data stored in the 
		/// conglomerate.
		/// </para>
		/// </remarks>
		public void Delete() {
			lock (CommitLock) {
				// We possibly have things to clean up.
				CleanUpConglomerate();

				// Go through and delete and close all the committed tables.
				foreach (MasterTableDataSource master in tableList)
					master.Drop();

				// Delete the state file
				stateStore.Commit();
				StoreSystem.CloseStore(actStateStore);
				StoreSystem.DeleteStore(actStateStore);

				// Delete the blob store
				if (blobStore != null) {
					StoreSystem.CloseStore(actBlobStore);
					StoreSystem.DeleteStore(actBlobStore);
				}

				// Invalidate this object
				tableList = null;

			}

			// Unlock the storage system.
			StoreSystem.Unlock(name);
		}


		/// <summary>
		/// Returns true if the conglomerate exists in the file system and can
		/// be opened.
		/// </summary>
		/// <returns></returns>
		public bool Exists() {
			return StoreSystem.StoreExists(name + StatePost);
		}

		/// <summary>
		/// Makes a complete copy of this database to the position represented 
		/// by the given TableDataConglomerate object.
		/// </summary>
		/// <param name="destConglomerate"></param>
		/// <remarks>
		/// The given TableDataConglomerate object must <b>not</b> be being 
		/// used by another database running in the environment. This may take 
		/// a while to complete. The backup operation occurs within its own 
		/// transaction and the copy transaction is read-only meaning there 
		/// is no way for the copy process to interfere with other transactions 
		/// running concurrently.
		/// <para>
		/// The conglomerate must be open before this method is called.
		/// </para>
		/// </remarks>
		public void LiveCopyTo(TableDataConglomerate destConglomerate) {
			// The destination store system
			IStoreSystem destStoreSystem = destConglomerate.StoreSystem;

			// Copy all the blob data from the given blob store to the current blob
			// store.
			destConglomerate.blobStore.CopyFrom(destStoreSystem, blobStore);

			// Open new transaction - this is the current view we are going to copy.
			Transaction transaction = CreateTransaction();

			try {
				// Copy the data in this transaction to the given destination store system.
				transaction.LiveCopyAllDataTo(destConglomerate);
			} finally {
				// Make sure we close the transaction
				try {
					transaction.Commit();
				} catch (TransactionException e) {
					throw new Exception("Transaction Error: " + e.Message);
				}
			}

			// Finished - increment the live copies counter.
			System.Stats.Increment("TableDataConglomerate.liveCopies");
		}

		// ---------- Transactional management ----------

		/// <summary>
		/// Closes and drops the <see cref="MasterTableDataSource"/>.
		/// </summary>
		/// <param name="tableFileName"></param>
		/// <remarks>
		/// This should only be called from the 
		/// <see cref="CleanUpConglomerate">clean up method</see>.
		/// <para>
		/// A drop may fail if, for example, the roots of the table are locked.
		/// </para>
		/// <para>
		/// Note that the table_file_name will be encoded with the table type.  
		/// For example, ":2mighty.db"
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns true if the drop succeeded.
		/// </returns>
		private bool CloseAndDropTable(string tableFileName) {
			// Find the table with this file name.
			for (int i = 0; i < tableList.Count; ++i) {
				MasterTableDataSource t = tableList[i];
				string encFn = tableFileName.Substring(2);
				if (t.SourceIdentity.Equals(encFn)) {
					// Close and remove from the list.
					if (t.IsRootLocked)
						// We can't drop a table that has roots locked..
						return false;

					// This drops if the table has been marked as being dropped.
					bool dropped = t.Drop();
					if (dropped)
						tableList.RemoveAt(i);
					return dropped;
				}
			}
			return false;
		}

		/// <summary>
		/// Closes the MasterTableDataSource with the given source ident.
		/// </summary>
		/// <param name="tableFileName"></param>
		/// <param name="pendingDrop"></param>
		/// <remarks>
		/// This should only be called from the 
		/// <see cref="CleanUpConglomerate">clean up method</see>.
		/// <para>
		/// Note that the table_file_name will be encoded with the table type.  
		/// For example, ":2mighty.db"
		/// </para>
		/// </remarks>
		private void CloseTable(string tableFileName, bool pendingDrop) {
			// Find the table with this file name.
			for (int i = 0; i < tableList.Count; ++i) {
				MasterTableDataSource t = tableList[i];
				string encFn = tableFileName.Substring(2);
				if (t.SourceIdentity.Equals(encFn)) {
					// Close and remove from the list.
					if (t.IsRootLocked)
						// We can't drop a table that has roots locked..
						return;

					// This closes the table
					t.Dispose(pendingDrop);
					return;
				}
			}
			return;
		}

		/// <summary>
		/// Cleans up the conglomerate by deleting all tables marked as deleted.
		/// </summary>
		/// <remarks>
		/// This should be called when the conglomerate is opened, shutdown and
		/// when there are no transactions open.
		/// </remarks>
		private void CleanUpConglomerate() {
			lock (CommitLock) {
				if (IsClosed)
					return;

				// If no open transactions on the database, then clean up.
				if (openTransactions.Count == 0) {
					StateStore.StateResource[] deleteList = stateStore.GetDeleteList();
					if (deleteList.Length > 0) {
						int dropCount = 0;

						for (int i = deleteList.Length - 1; i >= 0; --i) {
							String fn = deleteList[i].Name;
							CloseTable(fn, true);
						}

						for (int i = deleteList.Length - 1; i >= 0; --i) {
							string fn = deleteList[i].Name;
							bool dropped = CloseAndDropTable(fn);
							// If we managed to drop the table, remove from the list.
							if (dropped) {
								stateStore.RemoveDeleteResource(fn);
								++dropCount;
							}
						}

						// If we dropped a table, commit an update to the conglomerate state.
						if (dropCount > 0)
							stateStore.Commit();
					}
				}
			}
		}


		// ---------- IBlob store and object management ----------

		/// <summary>
		/// Creates and allocates storage for a new large object in the blob store.
		/// </summary>
		/// <param name="type"></param>
		/// <param name="size"></param>
		/// <remarks>
		/// This is called to create a new large object before filling it with 
		/// data sent from the client.
		/// </remarks>
		/// <returns></returns>
		internal IRef CreateNewLargeObject(ReferenceType type, long size) {
			try {
				// If the conglomerate is Read-only, a blob can not be created.
				if (IsReadOnly) {
					throw new Exception(
						"A new large object can not be allocated " +
						"with a Read-only conglomerate");
				}
				// Allocate the large object from the store
				IRef reference = blobStore.AllocateLargeObject(type, size);
				// Return the large object reference
				return reference;
			} catch (IOException e) {
				Logger.Error(this, e);
				throw new Exception("IO Error when creating blob: " +e.Message, e);
			}
		}

		/// <summary>
		/// Called when one or more blobs has been completed.
		/// </summary>
		/// <remarks>
		/// This flushes the blob to the blob store and completes the blob 
		/// write procedure. It's important this is called otherwise the 
		/// <see cref="BlobStore"/> may not be correctly flushed to disk with 
		/// the changes and the data will not be recoverable if a crash occurs.
		/// </remarks>
		[Obsolete("Deprecated: no longer necessary", false)]
		internal void FlushBlobStore() {
		}



		// ---------- low level File IO level operations on a conglomerate ----------
		// These operations are low level IO operations on the contents of the
		// conglomerate.  How the rows and tables are organised is up to the
		// transaction managemenet.  These methods deal with the low level
		// operations of creating/dropping tables and adding, deleting and querying
		// row in tables.

		/// <summary>
		/// Returns the <see cref="MasterTableDataSource"/> in this conglomerate 
		/// with the given table id.
		/// </summary>
		/// <param name="tableId"></param>
		/// <returns></returns>
		private MasterTableDataSource GetMasterTable(int tableId) {
			lock (CommitLock) {
				// Find the table with this table id.
				foreach (MasterTableDataSource t in tableList) {
					if (t.TableId == tableId)
						return t;
				}
				throw new ApplicationException("Unable to find an open table with id: " + tableId);
			}
		}

		/// <summary>
		/// Creates a table store in this conglomerate with the given name and 
		/// returns a reference to the table.
		/// </summary>
		/// <param name="tableInfo">The table meta definition.</param>
		/// <param name="dataSectorSize">The size of the data sectors 
		/// (affects performance and size of the file).</param>
		/// <param name="indexSectorSize">The size of the index sectors.</param>
		/// <remarks>
		/// Note that this table is not a commited change to the system. It 
		/// is a free standing blank table store. The table returned here is 
		/// uncommitted and will be deleted unless it is committed.
		/// <para>
		/// Note that two tables may exist within a conglomerate with the same 
		/// name, however each <b>committed</b> table must have a unique name.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		internal MasterTableDataSource CreateMasterTable(DataTableInfo tableInfo, int dataSectorSize, int indexSectorSize) {
			lock (CommitLock) {
				try {
					// EFFICIENCY: Currently this writes to the conglomerate state file
					//   twice.  Once in 'NextUniqueTableID' and once in
					//   'state_store.commit'.

					// The unique id that identifies this table,
					int tableId = NextUniqueTableID();

					// Create the object.
					V2MasterTableDataSource masterTable = new V2MasterTableDataSource(System, StoreSystem, blobStore);
					masterTable.Create(tableId, tableInfo);

					// Add to the list of all tables.
					tableList.Add(masterTable);

					// Add this to the list of deleted tables,
					// (This should really be renamed to uncommitted tables).
					MarkAsCommittedDropped(tableId);

					// Commit this
					stateStore.Commit();

					// And return it.
					return masterTable;
				} catch (IOException e) {
					Logger.Error(this, e);
					throw new ApplicationException("Unable to create master table '" + tableInfo.Name + "' - " + e.Message);
				}
			}
		}

		internal MasterTableDataSource CreateTemporaryDataSource(DataTableInfo tableInfo) {
			lock (CommitLock) {
				try {
					// The unique id that identifies this table,
					int tableId = NextUniqueTableID();

					V2MasterTableDataSource temporary = new V2MasterTableDataSource(System, tempStoreSystem, blobStore);
					temporary.Create(tableId, tableInfo);

					tableList.Add(temporary);

					return temporary;
				} catch(Exception e) {
					Logger.Error(this, e);
					throw new ApplicationException("Unable to create temporary table '" + tableInfo.Name + "' - " + e.Message);
				}
			}
		}

		/// <summary>
		/// Creates a table store in this conglomerate that is an exact copy 
		/// of the given <see cref="MasterTableDataSource"/>.
		/// </summary>
		/// <param name="srcMasterTable">The source master table to copy.</param>
		/// <param name="indexSet">The view of the table index to copy.</param>
		/// <remarks>
		/// Note that this table is not a commited change to the system. It is 
		/// a free standing blank table store. The table returned here is 
		/// uncommitted and will be deleted unless it is committed.
		/// <para>
		/// Note that two tables may exist within a conglomerate with the same 
		/// name, however each <b>committed</b> table must have a unique name.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns the <see cref="MasterTableDataSource"/> with the copied 
		/// information.
		/// </returns>
		internal MasterTableDataSource CopyMasterTable(MasterTableDataSource srcMasterTable, IIndexSet indexSet) {
			lock (CommitLock) {
				try {

					// EFFICIENCY: Currently this writes to the conglomerate state file
					//   twice.  Once in 'NextUniqueTableID' and once in
					//   'state_store.commit'.

					// The unique id that identifies this table,
					int tableId = NextUniqueTableID();

					// Create the object.
					V2MasterTableDataSource masterTable = new V2MasterTableDataSource(System, StoreSystem, blobStore);

					masterTable.CopyFrom(tableId, srcMasterTable, indexSet);

					// Add to the list of all tables.
					tableList.Add(masterTable);

					// Add this to the list of deleted tables,
					// (This should really be renamed to uncommitted tables).
					MarkAsCommittedDropped(tableId);

					// Commit this
					stateStore.Commit();

					// And return it.
					return masterTable;
				} catch (IOException e) {
					Logger.Error(this, e);
					throw new Exception("Unable to copy master table '" + srcMasterTable.TableInfo.Name + "' - " + e.Message);
				}
			}

		}

		// ---------- Inner classes ----------

		/// <summary>
		/// A variable resolver for a single row of a table source.
		/// </summary>
		/// <remarks>
		/// Used when evaluating a check constraint for newly added row.
		/// </remarks>
		private sealed class TableRowVariableResolver : IVariableResolver {

			private readonly ITableDataSource table;
			private readonly int rowIndex = -1;

			public TableRowVariableResolver(ITableDataSource table, int rowIndex) {
				this.table = table;
				this.rowIndex = rowIndex;
			}

			private int FindColumnName(VariableName variable) {
				int colIndex = table.TableInfo.FindColumnName(variable.Name);
				if (colIndex == -1)
					throw new ApplicationException("Can't find column: " + variable);

				return colIndex;
			}

			// --- Implemented ---

			public int SetId {
				get { return rowIndex; }
			}

			public TObject Resolve(VariableName variable) {
				int colIndex = FindColumnName(variable);
				return table.GetCell(colIndex, rowIndex);
			}

			public TType ReturnTType(VariableName variable) {
				int colIndex = FindColumnName(variable);
				return table.TableInfo[colIndex].TType;
			}
		}

		/// <summary>
		/// A journal for handling namespace clashes between transactions.
		/// </summary>
		/// <remarks>
		/// For example, we would need to generate a conflict if two concurrent
		/// transactions were to drop the same table, or if a procedure and a
		/// table with the same name were generated in concurrent transactions.
		/// </remarks>
		private sealed class NameSpaceJournal {
			/// <summary>
			/// The commit_id of this journal entry.
			/// </summary>
			public readonly long CommitId;

			/// <summary>
			/// The list of names created in this journal.
			/// </summary>
			public readonly IList<TableName> CreatedNames;

			/// <summary>
			/// The list of names dropped in this journal.
			/// </summary>
			public readonly IList<TableName> DroppedNames;

			public NameSpaceJournal(long commitId, IList<TableName> createdNames, IList<TableName> droppedNames) {
				CommitId = commitId;
				CreatedNames = createdNames;
				DroppedNames = droppedNames;
			}
		}


		#region Implementation of IDisposable

		public void Dispose() {
			//    removeShutdownHook();
		}

		#endregion
	}
}