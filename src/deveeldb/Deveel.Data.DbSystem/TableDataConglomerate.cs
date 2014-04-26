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

using Deveel.Data.Index;
using Deveel.Data.Store;
using Deveel.Data.Transactions;
using Deveel.Data.Types;
using Deveel.Data.Util;
using Deveel.Diagnostics;

namespace Deveel.Data.DbSystem {
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

		/**
		 * The schema info table.
		 */

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
		/// The <see cref="TypesManager"/> object for this conglomerate.
		/// </summary>
		// private readonly TypesManager typesManager;


		// ---------- Locks ----------

		/// <summary>
		/// This Lock is obtained when we go to commit a change to the table.
		/// </summary>
		/// <remarks>
		/// Grabbing this lock ensures that no other commits can occur at the same
		/// time on this conglomerate.
		/// </remarks>
		private readonly Object CommitLock = new Object();



		internal TableDataConglomerate(SystemContext context, string name, IStoreSystem storeSystem) {
			Context = context;
			Name = name;
			StoreSystem = storeSystem;

			// temporary tables live in memory
			tempStoreSystem = new V1HeapStoreSystem();

			openTransactions = new OpenTransactionList(context);
			modificationEvents = new EventHandlerList();
			namespaceJournalList = new List<NameSpaceJournal>();

			SequenceManager = new SequenceManager(this);
			// typesManager = new TypesManager(this);

		}

		/// <summary>
		/// Returns the <see cref="SystemContext"/> that this conglomerate is part of.
		/// </summary>
		public SystemContext Context { get; private set; }

		/// <summary>
		/// Returns the IStoreSystem used by this conglomerate to manage the persistent 
		/// state of the database.
		/// </summary>
		internal IStoreSystem StoreSystem { get; private set; }

		/// <summary>
		/// Returns the SequenceManager object for this conglomerate.
		/// </summary>
		internal SequenceManager SequenceManager { get; private set; }

		/*
		internal TypesManager TypesManager {
			get { return typesManager; }
		}
		*/

		/// <summary>
		/// Returns the BlobStore for this conglomerate.
		/// </summary>
		internal BlobStore BlobStore { get; private set; }

		/// <summary>
		/// Gets or sets the name given to this conglomerate.
		/// </summary>
		public string Name { get; private set; }

		internal ILogger Logger {
			get { return Context.Logger; }
		}

		// ---------- Conglomerate state methods ----------

		/// <summary>
		/// Returns true if the system is in read-only mode.
		/// </summary>
		private bool IsReadOnly {
			get { return Context.ReadOnlyAccess; }
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
				var master = new V2MasterTableDataSource(Context, StoreSystem, BlobStore);
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
		private int NextUniqueTableId() {
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
				throw new IOException("Conglomerate doesn't exists: " + Name);

			// Check the file Lock
			if (!IsReadOnly) {
				// Obtain the Lock (generate error if this is not possible)
				StoreSystem.Lock(Name);
			}

			// Open the state store
			actStateStore = StoreSystem.OpenStore(Name + StatePost);
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
				StoreSystem.SetCheckPoint();

				// Go through and close all the committed tables.
				foreach (MasterTableDataSource master in tableList) {
					master.Dispose(false);
				}

				stateStore.Commit();
				StoreSystem.CloseStore(actStateStore);

				tableList = null;
			}

			// Unlock the storage system
			StoreSystem.Unlock(Name);

			if (BlobStore != null)
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
				if (BlobStore != null) {
					StoreSystem.CloseStore(actBlobStore);
					StoreSystem.DeleteStore(actBlobStore);
				}

				// Invalidate this object
				tableList = null;

			}

			// Unlock the storage system.
			StoreSystem.Unlock(Name);
		}


		/// <summary>
		/// Returns true if the conglomerate exists in the file system and can
		/// be opened.
		/// </summary>
		/// <returns></returns>
		public bool Exists() {
			return StoreSystem.StoreExists(Name + StatePost);
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
			destConglomerate.BlobStore.CopyFrom(destStoreSystem, BlobStore);

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
			Context.Stats.Increment("TableDataConglomerate.liveCopies");
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
			foreach (MasterTableDataSource t in tableList) {
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
				IRef reference = BlobStore.AllocateLargeObject(type, size);
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
					int tableId = NextUniqueTableId();

					// Create the object.
					var masterTable = new V2MasterTableDataSource(Context, StoreSystem, BlobStore);
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
					int tableId = NextUniqueTableId();

					var temporary = new V2MasterTableDataSource(Context, tempStoreSystem, BlobStore);
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
					int tableId = NextUniqueTableId();

					// Create the object.
					var masterTable = new V2MasterTableDataSource(Context, StoreSystem, BlobStore);

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

		#region Diagnostics

		/// <summary>
		/// Given a table with a 'id' field, this will check that the sequence
		/// value for the table is at least greater than the maximum id in the column.
		/// </summary>
		/// <param name="tname"></param>
		private void ResetTableId(TableName tname) {
			// Create the transaction
			ICommitableTransaction transaction = CreateTransaction();
			// Get the table
			ITableDataSource table = transaction.GetTable(tname);
			// Find the index of the column name called 'id'
			DataTableInfo tableInfo = table.TableInfo;
			int colIndex = tableInfo.FindColumnName("id");
			if (colIndex == -1)
				throw new ApplicationException("Column name 'id' not found.");

			// Find the maximum 'id' value.
			SelectableScheme scheme = table.GetColumnScheme(colIndex);
			IList<int> list = scheme.SelectLast();
			if (list.Count > 0) {
				TObject value = table.GetCell(colIndex, list[0]);
				BigNumber bNum = value.ToBigNumber();
				if (bNum != null) {
					// Set the unique id to +1 the maximum id value in the column
					transaction.SetUniqueId(tname, bNum.ToInt64() + 1L);
				}
			}

			// Commit and close the transaction.
			try {
				transaction.Commit();
			} catch (TransactionException e) {
				Logger.Error(this, e);
				throw new ApplicationException("Transaction Exception creating conglomerate.", e);
			}
		}


		/// <summary>
		/// Resets the table sequence id for all the system tables managed by 
		/// the conglomerate.
		/// </summary>
		private void ResetAllSystemTableId() {
			ResetTableId(SystemSchema.PrimaryInfoTable);
			ResetTableId(SystemSchema.ForeignInfoTable);
			ResetTableId(SystemSchema.UniqueInfoTable);
			ResetTableId(SystemSchema.CheckInfoTable);
			ResetTableId(SystemSchema.SchemaInfoTable);
		}

		/// <summary>
		/// Checks the list of committed tables in this conglomerate.
		/// </summary>
		/// <param name="terminal"></param>
		/// <remarks>
		/// This should only be called during an 'check' like method.  This method 
		/// fills the 'committed_tables' and 'table_list' lists with the tables in 
		/// this conglomerate.
		/// </remarks>
		public void CheckVisibleTables(IUserTerminal terminal) {
			// The list of all visible tables from the state file
			IEnumerable<StateStore.StateResource> tables = stateStore.GetVisibleList();
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

				if (!(master is V2MasterTableDataSource))
					throw new ApplicationException("Unknown master table type: " + master.GetType());

				V2MasterTableDataSource v2Master = (V2MasterTableDataSource)master;
				v2Master.SourceIdentity = fileName;
				v2Master.Repair(terminal);

				// Add the table to the table list
				tableList.Add(master);

				// Set a check point
				StoreSystem.SetCheckPoint();
			}
		}


		/// <summary>
		/// Checks the conglomerate state file.
		/// </summary>
		/// <param name="terminal"></param>
		public void Fix(IUserTerminal terminal) {
			try {
				string stateFn = (Name + StatePost);
				bool stateExists = false;
				try {
					stateExists = Exists();
				} catch (IOException e) {
					terminal.WriteLine("IO Error when checking if state store exists: " + e.Message);
					Console.Error.WriteLine(e.StackTrace);
				}

				if (!stateExists) {
					terminal.WriteLine("Couldn't find store: " + stateFn);
					return;
				}
				terminal.WriteLine("+ Found state store: " + stateFn);

				// Open the state store
				try {
					actStateStore = StoreSystem.OpenStore(Name + StatePost);
					stateStore = new StateStore(actStateStore);
					// Get the 64 byte fixed area
					IArea fixed_area = actStateStore.GetArea(-1);
					long head_p = fixed_area.ReadInt8();
					stateStore.Init(head_p);
					terminal.WriteLine("+ Initialized the state store: " + stateFn);
				} catch (IOException e) {
					// Couldn't initialize the state file.
					terminal.WriteLine("Couldn't initialize the state file: " + stateFn +
									 " Reason: " + e.Message);
					return;
				}

				// Initialize the blob store
				try {
					InitializeBlobStore();
				} catch (IOException e) {
					terminal.WriteLine("Error intializing BlobStore: " + e.Message);
					Console.Error.WriteLine(e.StackTrace);
					return;
				}
				// Setup internal
				SetupInternal();

				try {
					CheckVisibleTables(terminal);

					// Reset the sequence id's for the system tables
					terminal.WriteLine("+ RESETTING ALL SYSTEM TABLE UNIQUE ID VALUES.");
					ResetAllSystemTableId();

					// Some diagnostic information
					IEnumerable<StateStore.StateResource> committedTables = stateStore.GetVisibleList();
					IEnumerable<StateStore.StateResource> committedDropped = stateStore.GetDeleteList();
					foreach (StateStore.StateResource resource in committedTables) {
						terminal.WriteLine("+ COMMITTED TABLE: " + resource.Name);
					}
					foreach (StateStore.StateResource resource in committedDropped) {
						terminal.WriteLine("+ COMMIT DROPPED TABLE: " + resource.Name);
					}

					return;

				} catch (IOException e) {
					terminal.WriteLine("IOException: " + e.Message);
					Console.Out.WriteLine(e.StackTrace);
				}

			} finally {
				try {
					Close();
				} catch (IOException) {
					terminal.WriteLine("Unable to close conglomerate after fix.");
				}
			}
		}

		/// <summary>
		/// Returns a RawDiagnosticTable object that is used for diagnostics of 
		/// the table with the given file name.
		/// </summary>
		/// <param name="tableFileName"></param>
		/// <returns></returns>
		public IRawDiagnosticTable GetDiagnosticTable(string tableFileName) {
			lock (CommitLock) {
				foreach (MasterTableDataSource master in tableList) {
					if (master.SourceIdentity.Equals(tableFileName)) {
						return master.GetRawDiagnosticTable();
					}
				}
			}
			return null;
		}

		///<summary>
		/// Returns the list of file names for all tables in this conglomerate.
		///</summary>
		///<returns></returns>
		public String[] GetAllTableFileNames() {
			lock (CommitLock) {
				String[] list = new String[tableList.Count];
				for (int i = 0; i < tableList.Count; ++i) {
					MasterTableDataSource master = tableList[i];
					list[i] = master.SourceIdentity;
				}
				return list;
			}
		}

		#endregion

		#region Transactions

		/// <summary>
		/// The list of transactions that are currently open over this conglomerate.
		/// </summary>
		/// <remarks>
		/// This list is ordered from lowest commit_id to highest.  This object is
		/// shared with all the children MasterTableDataSource objects.
		/// </remarks>
		private readonly OpenTransactionList openTransactions;

		/// <summary>
		/// Starts a new transaction.
		/// </summary>
		/// <remarks>
		/// The <see cref="Transaction"/> object returned by this method is 
		/// used to read the contents of the database at the time the transaction 
		/// was started. It is also used if any modifications are required to 
		/// be made.
		/// </remarks>
		/// <returns></returns>
		internal Transaction CreateTransaction() {
			List<MasterTableDataSource> thisCommittedTables = new List<MasterTableDataSource>();

			// Don't let a commit happen while we are looking at this.
			lock (CommitLock) {
				long thisCommitId = commitId;
				IEnumerable<StateStore.StateResource> committedTableList = stateStore.GetVisibleList();
				foreach (StateStore.StateResource resource in committedTableList) {
					thisCommittedTables.Add(GetMasterTable((int)resource.TableId));
				}

				// Create a set of IIndexSet for all the tables in this transaction.
				int sz = thisCommittedTables.Count;
				List<IIndexSet> indexInfo = new List<IIndexSet>(sz);
				foreach (MasterTableDataSource mtable in thisCommittedTables) {
					indexInfo.Add(mtable.CreateIndexSet());
				}

				// Create the transaction and record it in the open transactions list.
				Transaction t = new Transaction(this, thisCommitId, thisCommittedTables, indexInfo);
				openTransactions.AddTransaction(t);
				return t;
			}
		}

		/// <summary>
		/// This is called to notify the conglomerate that the transaction has
		/// closed.
		/// </summary>
		/// <param name="transaction"></param>
		/// <remarks>
		/// This is always called from either the rollback or commit method
		/// of the transaction object.
		/// <para>
		/// <b>Note</b> This increments 'commit_id' and requires that the 
		/// conglomerate is commit locked.
		/// </para>
		/// </remarks>
		private void CloseTransaction(Transaction transaction) {
			bool lastTransaction;
			// Closing must happen under a commit Lock.
			lock (CommitLock) {
				openTransactions.RemoveTransaction(transaction);
				// Increment the commit id.
				++commitId;
				// Was that the last transaction?
				lastTransaction = openTransactions.Count == 0;
			}

			// If last transaction then schedule a clean up event.
			if (lastTransaction) {
				try {
					CleanUpConglomerate();
				} catch (IOException e) {
					Logger.Error(this, "Error cleaning up conglomerate");
					Logger.Error(this, e);
				}
			}

		}

		#endregion
	}
}