//  
//  MasterTableDataSource.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.IO;
using System.Text;

using Deveel.Data.Caching;
using Deveel.Data.Collections;
using Deveel.Data.Store;
using Deveel.Data.Store;

using Deveel.Diagnostics;

namespace Deveel.Data {
	/// <summary>
	/// A master table data source provides facilities for read/writing and
	/// maintaining low level data in a table.
	/// </summary>
	/// <remarks>
	/// It provides primitive table operations such as retrieving a cell from 
	/// a table, accessing the table's <see cref="DataTableDef"/>, accessing 
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
	abstract class MasterTableDataSource {


		// ---------- System information ----------

		/// <summary>
		/// The global TransactionSystem object that points to the global system
		/// that this table source belongs to.
		/// </summary>
		private TransactionSystem system;

		/// <summary>
		/// The IStoreSystem implementation that represents the data persistence layer.
		/// </summary>
		private IStoreSystem store_system;

		// ---------- State information ----------

		/// <summary>
		/// An integer that uniquely identifies this data source within the conglomerate.
		/// </summary>
		protected int table_id;

		/// <summary>
		/// True if this table source is closed.
		/// </summary>
		protected bool is_closed;

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
		private int root_lock;

		// ---------- Persistant data ----------

		/// <summary>
		/// A DataTableDef object that describes the table topology.  This includes
		/// the name and columns of the table.
		/// </summary>
		protected DataTableDef table_def;

		/// <summary>
		/// A DataIndexSetDef object that describes the indexes on the table.
		/// </summary>
		protected DataIndexSetDef index_def;

		/// <summary>
		/// A cached TableName for this data source.
		/// </summary>
		private TableName cached_table_name;

		/// <summary>
		/// A multi-version representation of the table indices kept for this table
		/// including the row list and the scheme indices.  This contains the
		/// transaction journals.
		/// </summary>
		protected MultiVersionTableIndices table_indices;

		/// <summary>
		/// The list of RIDList objects for each column in this table.  This is
		/// a sorting optimization.
		/// </summary>
		// protected RIDList[] column_rid_list;

		// ---------- Cached information ----------

		/// <summary>
		/// Set to false to disable cell caching.
		/// </summary>
		protected bool DATA_CELL_CACHING = true;

		/// <summary>
		/// A reference to the DataCellCache object.
		/// </summary>
		protected readonly DataCellCache cache;

		/// <summary>
		/// The number of columns in this table.  This is a cached optimization.
		/// </summary>
		protected int column_count;



		// --------- Parent information ----------

		/// <summary>
		/// The list of all open transactions managed by the parent conglomerate.
		/// </summary>
		/// <remarks>
		/// This is a thread safe object, and is updated whenever new transactions
		/// are created, or transactions are closed.
		/// </remarks>
		private OpenTransactionList open_transactions;

		// ---------- Row garbage collection ----------

		/// <summary>
		/// Manages scanning and deleting of rows marked as deleted within this
		/// data source.
		/// </summary>
		protected MasterTableGC gc;

		// ---------- IBlob management ----------

		/// <summary>
		/// An abstracted reference to a BlobStore for managing blob 
		/// references and blob data.
		/// </summary>
		protected IBlobStore blob_store;

		// ---------- Stat keys ----------

		// The keys we use for Database.stats() for information for this table.
		protected String root_lock_key;
		protected String total_hits_key;
		protected String file_hits_key;
		protected String delete_hits_key;
		protected String insert_hits_key;

		/// <summary>
		/// Constructs the <see cref="MasterTableDataSource"/>.
		/// </summary>
		/// <param name="system"></param>
		/// <param name="store_system"></param>
		/// <param name="open_transactions">An object that manages the list of 
		/// open transactions in the conglomerate.</param>
		/// <param name="blob_store"></param>
		internal MasterTableDataSource(TransactionSystem system, IStoreSystem store_system,
			OpenTransactionList open_transactions, IBlobStore blob_store) {
			this.system = system;
			this.store_system = store_system;
			this.open_transactions = open_transactions;
			this.blob_store = blob_store;
			this.gc = new MasterTableGC(this);
			this.cache = system.DataCellCache;
			is_closed = true;

			if (DATA_CELL_CACHING) {
				DATA_CELL_CACHING = (cache != null);
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
		/*
		TODO:
		public IDebugLogger Debug {
			get { return System.Debug; }
		}
		*/

		/// <summary>
		/// Returns the TableName of this table source.
		/// </summary>
		public TableName TableName {
			get { return DataTableDef.TableName; }
		}

		/// <summary>
		/// Returns the name of this table source.
		/// </summary>
		public string Name {
			get { return DataTableDef.Name; }
		}

		/// <summary>
		/// Returns the schema name of this table source.
		/// </summary>
		public string Schema {
			get { return DataTableDef.Schema; }
		}

		/// <summary>
		/// Returns a cached TableName for this data source.
		/// </summary>
		internal TableName CachedTableName {
			get {
				lock (this) {
					if (cached_table_name != null) {
						return cached_table_name;
					}
					cached_table_name = TableName;
					return cached_table_name;
				}
			}
		}

		/// <summary>
		/// Updates the master records from the journal logs up to the given
		/// <paramref name="commit_id"/>.
		/// </summary>
		/// <param name="commit_id"></param>
		/// <remarks>
		/// This could be a fairly expensive operation if there are a lot of 
		/// modifications because each change could require a lookup of 
		/// records in the data source.
		/// <para>
		/// <b>Note</b>: It's extremely important that when this is called, 
		/// there are no transaction open that are using the merged journal. 
		/// If there is, then a transaction may be able to see changes in a 
		/// table that were made after the transaction started.
		/// </para>
		/// <para>
		/// After this method is called, it's best to update the index file
		/// with a call to 'synchronizeIndexFiles'
		/// </para>
		/// </remarks>
		internal void MergeJournalChanges(long commit_id) {
			lock (this) {
				bool all_merged = table_indices.MergeJournalChanges(commit_id);
				// If all journal entries merged then schedule deleted row collection.
				if (all_merged && !IsReadOnly) {
					checkForCleanup();
				}
			}
		}

		/// <summary>
		/// Returns a list of all <see cref="MasterTableJournal"/> objects that 
		/// have been successfully committed against this table that have an 
		/// <paramref name="commit_id"/> that is greater or equal to the given.
		/// </summary>
		/// <param name="commit_id"></param>
		/// <remarks>
		/// This is part of the conglomerate commit check phase and will be 
		/// on a commit_lock.
		/// </remarks>
		/// <returns></returns>
		internal MasterTableJournal[] FindAllJournalsSince(long commit_id) {
			lock (this) {
				return table_indices.FindAllJournalsSince(commit_id);
			}
		}

		// ---------- Getters ----------

		/// <summary>
		/// Returns table_id - the unique identifier for this data source.
		/// </summary>
		internal int TableID {
			get { return table_id; }
		}

		/// <summary>
		/// Returns the DataTableDef object that represents the topology of this
		/// table data source (name, columns, etc).
		/// </summary>
		/// <remarks>
		/// This information can't be changed during the lifetime of a data source.
		/// </remarks>
		internal DataTableDef DataTableDef {
			get { return table_def; }
		}

		/// <summary>
		/// Returns the DataIndexSetDef object that represents the indexes on this table.
		/// </summary>
		internal DataIndexSetDef DataIndexSetDef {
			get { return index_def; }
		}

		public IDebugLogger Debug {
			get { return System.Debug; }
		}

		// ---------- Convenient statics ----------

		/// <summary>
		/// Creates a unique table name to give a file.
		/// </summary>
		/// <param name="system"></param>
		/// <param name="table_id">A guarenteed unique number between all tables.</param>
		/// <param name="table_name"></param>
		/// <remarks>
		/// This could be changed to suit a particular OS's style of filesystem 
		/// namespace. Or it could return some arbitarily unique number. 
		/// However, for debugging purposes it's often a good idea to return a 
		/// name that a user can recognise.
		/// </remarks>
		/// <returns></returns>
		protected static String MakeTableFileName(TransactionSystem system,
												int table_id, TableName table_name) {

			// NOTE: We may want to change this for different file systems.
			//   For example DOS is not able to handle more than 8 characters
			//   and is case insensitive.
			String tid = table_id.ToString();
			int pad = 3 - tid.Length;
			StringBuilder buf = new StringBuilder();
			for (int i = 0; i < pad; ++i) {
				buf.Append('0');
			}

			String str = table_name.ToString().Replace('.', '_');

			// Go through each character and remove each non a-z,A-Z,0-9,_ character.
			// This ensure there are no strange characters in the file name that the
			// underlying OS may not like.
			StringBuilder osified_name = new StringBuilder();
			int count = 0;
			for (int i = 0; i < str.Length || count > 64; ++i) {
				char c = str[i];
				if ((c >= 'a' && c <= 'z') ||
					(c >= 'A' && c <= 'Z') ||
					(c >= '0' && c <= '9') ||
					c == '_') {
					osified_name.Append(c);
					++count;
				}
			}

			return buf.ToString() + tid + osified_name.ToString();
		}


		// ---------- Abstract methods ----------

		/// <summary>
		/// Returns a string that uniquely identifies this table within the
		/// conglomerate context.
		/// </summary>
		/// <remarks>
		/// For example, the filename of the table.  This string can be used 
		/// to open and initialize the table also.
		/// </remarks>
		internal abstract string SourceIdentity { get; }

		/// <summary>
		/// Sets the record type for the given record in the table and returns 
		/// the previous state of the record.
		/// </summary>
		/// <param name="row_index"></param>
		/// <param name="row_state"></param>
		/// <remarks>
		/// This is used to change the state of a row in the table.
		/// </remarks>
		/// <returns></returns>
		internal abstract int WriteRecordType(int row_index, int row_state);

		/// <summary>
		/// Reads the record state for the given record in the table.
		/// </summary>
		/// <param name="row_index"></param>
		/// <returns></returns>
		internal abstract int ReadRecordType(int row_index);

		/// <summary>
		/// Returns true if the record with the given index is deleted from the 
		/// table.
		/// </summary>
		/// <param name="row_index"></param>
		/// <remarks>
		/// A deleted row can not be read.
		/// </remarks>
		/// <returns></returns>
		internal abstract bool RecordDeleted(int row_index);

		/// <summary>
		/// Returns the raw count or rows in the table, including uncommited,
		/// committed and deleted rows.
		/// </summary>
		/// <remarks>
		/// This is basically the maximum number of rows we can iterate through.
		/// </remarks>
		internal abstract int RawRowCount { get; }

		/// <summary>
		/// Removes the row at the given index so that any resources associated
		/// with the row may be immediately available to be recycled.
		/// </summary>
		/// <param name="row_index"></param>
		internal abstract void InternalDeleteRow(int row_index);

		/// <summary>
		/// Creates and returns an <see cref="IIndexSet"/> object that is used 
		/// to create indices for this table source.
		/// </summary>
		/// <remarks>
		/// The <see cref="IIndexSet"/> represents a snapshot of the table and 
		/// the given point in time.
		/// <para>
		/// <b>Note</b> Not synchronized because we synchronize in the 
		/// <see cref="IndexSetStore"/> object.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		internal abstract IIndexSet CreateIndexSet();

		/// <summary>
		/// Commits changes made to an IndexSet returned by the 
		/// <see cref="CreateIndexSet"/> method.
		/// </summary>
		/// <param name="index_set"></param>
		/// <remarks>
		/// This method also disposes the IndexSet so it is no longer valid.
		/// </remarks>
		internal abstract void CommitIndexSet(IIndexSet index_set);

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
		internal abstract int InternalAddRow(RowData data);

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
		internal abstract TObject InternalGetCellContents(int column, int row);

		/// <summary>
		/// Atomically returns the current 'unique_id' value for this table.
		/// </summary>
		internal abstract long CurrentUniqueId { get; }

		/// <summary>
		/// Atomically returns the next 'unique_id' value from this table.
		/// </summary>
		internal abstract long NextUniqueId { get; }

		/// <summary>
		/// Sets the unique id for this store.
		/// </summary>
		/// <param name="value"></param>
		/// <remarks>
		/// This must only be used under extraordinary circumstances, such as 
		/// restoring from a backup, or converting from one file to another.
		/// </remarks>
		internal abstract void SetUniqueID(long value);

		/// <summary>
		/// Disposes of all in-memory resources associated with this table and
		/// invalidates this object.
		/// </summary>
		/// <param name="pending_drop">If true the table is to be disposed 
		/// pending a call to <see cref="Drop"/> and any persistant resources 
		/// that are allocated may be freed.</param>
		internal abstract void Dispose(bool pending_drop);

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
		internal abstract bool Drop();

		/// <summary>
		/// Called by the 'shutdown hook' on the conglomerate.
		/// </summary>
		/// <remarks>
		/// This method should block until the table can by write into a safe 
		/// mode and then prevent any further access to the object after it 
		/// returns.  It must operate very quickly.
		/// </remarks>
		internal abstract void ShutdownHookCleanup();



		/// <summary>
		/// Returns true if a compact table is necessary.
		/// </summary>
		/// <remarks>
		/// By default, we return true however it is recommended this method 
		/// is overwritten and the table tested.
		/// </remarks>
		internal virtual bool Compact {
			get { return true; }
		}

		/// <summary>
		/// Creates a <see cref="SelectableScheme"/> object for the given 
		/// column in this table.
		/// </summary>
		/// <param name="index_set"></param>
		/// <param name="table"></param>
		/// <param name="column"></param>
		/// <remarks>
		/// This reads the index from the index set (if there is one) then wraps
		/// it around the selectable schema as appropriate.
		/// <para>
		/// <b>Note</b>: This needs to be deprecated in support of composite 
		/// indexes.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		internal SelectableScheme CreateSelectableSchemeForColumn(IIndexSet index_set, ITableDataSource table, int column) {
			lock (this) {
				// What's the type of scheme for this column?
				DataTableColumnDef column_def = DataTableDef[column];

				// If the column isn't indexable then return a BlindSearch object
				if (!column_def.IsIndexableType) {
					return new BlindSearch(table, column);
				}

				String scheme_type = column_def.IndexScheme;
				if (scheme_type.Equals("InsertSearch")) {
					// Search the TableIndexDef for this column
					DataIndexSetDef index_set_def = DataIndexSetDef;
					int index_i = index_set_def.FindIndexForColumns(new String[] { column_def.Name });
					return CreateSelectableSchemeForIndex(index_set, table, index_i);
				} else if (scheme_type.Equals("BlindSearch")) {
					return new BlindSearch(table, column);
				} else {
					throw new ApplicationException("Unknown scheme type");
				}
			}
		}

		/// <summary>
		/// Creates a SelectableScheme object for the given index in the index 
		/// set def in this table.
		/// </summary>
		/// <param name="index_set"></param>
		/// <param name="table"></param>
		/// <param name="index_i"></param>
		/// <remarks>
		/// This reads the index from the index set (if there is one) then 
		/// wraps it around the selectable schema as appropriate.
		/// </remarks>
		/// <returns></returns>
		internal SelectableScheme CreateSelectableSchemeForIndex(IIndexSet index_set, ITableDataSource table, int index_i) {
			lock (this) {
				// Get the IndexDef object
				DataIndexDef index_def = DataIndexSetDef[index_i];

				if (index_def.Type.Equals("BLIST")) {
					String[] cols = index_def.ColumnNames;
					DataTableDef table_def = DataTableDef;
					if (cols.Length == 1) {
						// If a single column
						int col_index = table_def.FindColumnName(cols[0]);
						// Get the index from the index set and set up the new InsertSearch
						// scheme.
						IIntegerList index_list =
							index_set.GetIndex(index_def.Pointer);
						InsertSearch iis = new InsertSearch(table, col_index, index_list);
						return iis;
					} else {
						throw new Exception(
							"Multi-column indexes not supported at this time.");
					}
				} else {
					throw new Exception("Unrecognised type.");
				}

			}
		}

		/// <summary>
		/// Creates a minimal <see cref="ITableDataSource"/> implementation 
		/// that represents this <see cref="MasterTableDataSource"/>.
		/// </summary>
		/// <param name="master_index"></param>
		/// <remarks>
		/// The implementation returned does not implement the 
		/// <see cref="ITableDataSource.GetColumnScheme"/> method.
		/// </remarks>
		/// <returns></returns>
		protected ITableDataSource MinimalTableDataSource(IIntegerList master_index) {
			// Make a ITableDataSource that represents the master table over this
			// index.
			return new TableDataSourceImpl(this, master_index);
		}

		private class TableDataSourceImpl : ITableDataSource {
			private readonly MasterTableDataSource mtds;
			private readonly IIntegerList master_index;

			public TableDataSourceImpl(MasterTableDataSource mtds, IIntegerList master_index) {
				this.mtds = mtds;
				this.master_index = master_index;
			}

			public TransactionSystem System {
				get { return mtds.system; }
			}

			public DataTableDef DataTableDef {
				get { return mtds.DataTableDef; }
			}

			public int RowCount {
				get {
					// NOTE: Returns the number of rows in the master index before journal
					//   entries have been made.
					return master_index.Count;
				}
			}

			public IRowEnumerator GetRowEnumerator() {
					// NOTE: Returns iterator across master index before journal entry
					//   changes.
					// Get an iterator across the row list.
					IIntegerIterator iterator = master_index.GetIterator();
					// Wrap it around a IRowEnumerator object.
					return new RowEnumerationImpl(iterator);
			}

			private class RowEnumerationImpl : IRowEnumerator {
				public RowEnumerationImpl(IIntegerIterator iterator) {
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
			public SelectableScheme GetColumnScheme(int column) {
				throw new ApplicationException("Not implemented.");
			}
			public TObject GetCellContents(int column, int row) {
				return mtds.GetCellContents(column, row);
			}
		}

		/// <summary>
		/// Builds a complete index set on the data in this table.
		/// </summary>
		/// <remarks>
		/// This must only be called when either:
		/// <list type="bullet">
		/// <item>we are under a commit lock</item>
		/// <item>there is a guarentee that no concurrect access to the indexing 
		/// information can happen (such as when we are creating the table).</item>
		/// </list>
		/// <para>
		/// <b>Note</b> We assume that the index information for this table is 
		/// blank before this method is called.
		/// </para>
		/// </remarks>
		internal void BuildIndexes() {
			lock (this) {
				IIndexSet index_set = CreateIndexSet();

				DataIndexSetDef index_set_def = DataIndexSetDef;

				int row_count = RawRowCount;

				// Master index is always on index position 0
				IIntegerList master_index = index_set.GetIndex(0);

				// First, update the master index
				for (int row_index = 0; row_index < row_count; ++row_index) {
					// If this row isn't deleted, set the index information for it,
					if (!RecordDeleted(row_index)) {
						// First add to master index
						bool inserted = master_index.UniqueInsertSort(row_index);
						if (!inserted) {
							throw new Exception(
								"Assertion failed: Master index entry was duplicated.");
						}
					}
				}

				// Commit the master index
				CommitIndexSet(index_set);

				// Now go ahead and build each index in this table
				int index_count = index_set_def.IndexCount;
				for (int i = 0; i < index_count; ++i) {
					BuildIndex(i);
				}

			}
		}

		/// <summary>
		/// Builds the given index number (from the <see cref="DataIndexSetDef"/>).
		/// </summary>
		/// <param name="index_number"></param>
		/// <remarks>
		/// This must only be called when either:
		/// <list type="bullet">
		/// <item>we are under a commit lock</item>
		/// <item>there is a guarentee that no concurrect access to the indexing 
		/// information can happen (such as when we are creating the table).</item>
		/// </list>
		/// <para>
		/// <b>Note</b> We assume that the index number in this table is blank before this
		/// method is called.
		/// </para>
		/// </remarks>
		internal void BuildIndex(int index_number) {
			lock (this) {
				DataIndexSetDef index_set_def = DataIndexSetDef;

				IIndexSet index_set = CreateIndexSet();

				// Master index is always on index position 0
				IIntegerList master_index = index_set.GetIndex(0);
				// A minimal ITableDataSource for constructing the indexes
				ITableDataSource min_table_source = MinimalTableDataSource(master_index);

				// Set up schemes for the index,
				SelectableScheme scheme = CreateSelectableSchemeForIndex(index_set,
																		 min_table_source, index_number);

				// Rebuild the entire index
				int row_count = RawRowCount;
				for (int row_index = 0; row_index < row_count; ++row_index) {

					// If this row isn't deleted, set the index information for it,
					if (!RecordDeleted(row_index)) {
						scheme.Insert(row_index);
					}

				}

				// Commit the index
				CommitIndexSet(index_set);

			}
		}


		/// <summary>
		/// Adds a new transaction modification to this master table source.
		/// </summary>
		/// <param name="commit_id"></param>
		/// <param name="change"></param>
		/// <param name="index_set">Represents the changed index information to 
		/// commit to this table.</param>
		/// <remarks>
		/// This information represents the information that was added/removed 
		/// in the table in this transaction.
		/// <para>
		/// It's guarenteed that 'commit_id' additions will be sequential.
		/// </para>
		/// </remarks>
		internal void CommitTransactionChange(long commit_id,
									MasterTableJournal change, IIndexSet index_set) {
			lock (this) {
				// ASSERT: Can't do this if source is Read only.
				if (IsReadOnly) {
					throw new ApplicationException("Can't commit transaction journal, table is Read only.");
				}

				change.CommitId = commit_id;

				try {

					// Add this journal to the multi version table indices log
					table_indices.AddTransactionJournal(change);

					// Write the modified index set to the index store
					// (Updates the index file)
					CommitIndexSet(index_set);

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

					int size = change.EntriesCount;
					for (int i = 0; i < size; ++i) {
						byte b = change.GetCommand(i);
						int row_index = change.GetRowIndex(i);
						// Was a row added or removed?
						if (MasterTableJournal.IsAddCommand(b)) {

							// Record commit added
							int old_type = WriteRecordType(row_index, 0x010);
							// Check the record was in an uncommitted state before we changed
							// it.
							if ((old_type & 0x0F0) != 0) {
								WriteRecordType(row_index, old_type & 0x0F0);
								throw new ApplicationException("Record " + row_index + " of table " + this +
												" was not in an uncommitted state!");
							}

						} else if (MasterTableJournal.IsRemoveCommand(b)) {

							// Record commit removed
							int old_type = WriteRecordType(row_index, 0x020);
							// Check the record was in an added state before we removed it.
							if ((old_type & 0x0F0) != 0x010) {
								WriteRecordType(row_index, old_type & 0x0F0);
								//            Console.Out.WriteLine(change);
								throw new ApplicationException("Record " + row_index + " of table " + this +
												" was not in an added state!");
							}
							// Notify collector that this row has been marked as deleted.
							gc.MarkRowAsDeleted(row_index);

						}
					}

				} catch (IOException e) {
					Debug.WriteException(e);
					throw new ApplicationException("IO Error: " + e.Message);
				}

			}
		}

		/// <summary>
		/// Rolls back a transaction change in this table source.
		/// </summary>
		/// <param name="change"></param>
		/// <remarks>
		/// Any rows added to the table will be uncommited rows (type_key = 0).  
		/// Those rows must be marked as committed deleted.
		/// </remarks>
		internal void RollbackTransactionChange(MasterTableJournal change) {
			lock (this) {
				// ASSERT: Can't do this is source is Read only.
				if (IsReadOnly) {
					throw new ApplicationException(
						"Can't rollback transaction journal, table is Read only.");
				}

				// Any rows added in the journal are marked as committed deleted and the
				// journal is then discarded.

				try {
					// Mark all rows in the data_store as appropriate to the changes.
					int size = change.EntriesCount;
					for (int i = 0; i < size; ++i) {
						byte b = change.GetCommand(i);
						int row_index = change.GetRowIndex(i);
						// Make row as added or removed.
						if (MasterTableJournal.IsAddCommand(b)) {
							// Record commit removed (we are rolling back remember).
							//          int old_type = data_store.WriteRecordType(row_index + 1, 0x020);
							int old_type = WriteRecordType(row_index, 0x020);
							// Check the record was in an uncommitted state before we changed
							// it.
							if ((old_type & 0x0F0) != 0) {
								//            data_store.WriteRecordType(row_index + 1, old_type & 0x0F0);
								WriteRecordType(row_index, old_type & 0x0F0);
								throw new ApplicationException("Record " + row_index + " was not in an " +
												"uncommitted state!");
							}
							// Notify collector that this row has been marked as deleted.
							gc.MarkRowAsDeleted(row_index);
						} else if (MasterTableJournal.IsRemoveCommand(b)) {
							// Any journal entries marked as TABLE_REMOVE are ignored because
							// we are rolling back.  This means the row is not logically changed.
						}
					}

					// The journal entry is discarded, the indices do not need to be updated
					// to reflect this rollback.
				} catch (IOException e) {
					Debug.WriteException(e);
					throw new ApplicationException("IO Error: " + e.Message);
				}
			}
		}

		/// <summary>
		/// Returns a <see cref="IMutableTableDataSource"/> object that represents 
		/// this data source at the time the given transaction started.
		/// </summary>
		/// <param name="transaction"></param>
		/// <remarks>
		/// Any modifications to the returned table are logged in the table 
		/// journal.
		/// <para>
		/// This is a key method in this object because it allows us to get a 
		/// data source that represents the data in the table before any 
		/// modifications may have been committed.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		internal IMutableTableDataSource CreateTableDataSourceAtCommit(
													SimpleTransaction transaction) {
			return CreateTableDataSourceAtCommit(transaction,
												 new MasterTableJournal(TableID));
		}

		/// <summary>
		/// Returns a <see cref="IMutableTableDataSource"/> object that represents 
		/// this data source at the time the given transaction started, and also 
		/// makes any modifications that are described by the journal in the 
		/// table.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="journal"></param>
		/// <remarks>
		/// This method is useful for merging the changes made by a transaction 
		/// into a view of the table.
		/// </remarks>
		/// <returns></returns>
		internal IMutableTableDataSource CreateTableDataSourceAtCommit(SimpleTransaction transaction, MasterTableJournal journal) {
			return new MMutableTableDataSource(this, transaction, journal);
		}

		// ---------- File IO level table modification ----------

		/// <summary>
		/// Sets up the <see cref="DataIndexSetDef"/> object from the information 
		/// set in this object
		/// </summary>
		/// <remarks>
		/// This will only setup a default <see cref="DataIndexSetDef"/> on the 
		/// information in the <see cref="DataTableDef"/>.
		/// </remarks>
		protected void SetupDataIndexSetDef() {
			lock (this) {
				// Create the initial DataIndexSetDef object.
				index_def = new DataIndexSetDef(table_def.TableName);
				for (int i = 0; i < table_def.ColumnCount; ++i) {
					DataTableColumnDef col_def = table_def[i];
					if (col_def.IsIndexableType &&
						col_def.IndexScheme.Equals("InsertSearch")) {
						index_def.AddDataIndexDef(new DataIndexDef("ANON-COLUMN:" + i,
																   new String[] { col_def.Name }, i + 1,
																   "BLIST", false));
					}
				}
			}
		}

		/// <summary>
		/// Sets up the DataTableDef.
		/// </summary>
		/// <param name="table_def"></param>
		/// <remarks>
		/// This would typically only ever be called from the <i>create</i>
		/// method.
		/// </remarks>
		protected void SetupDataTableDef(DataTableDef table_def) {
			lock (this) {
				// Check table_id isn't too large.
				if ((table_id & 0x0F0000000) != 0) {
					throw new ApplicationException("'table_id' exceeds maximum possible keys.");
				}

				this.table_def = table_def;

				// The name of the table to create,
				TableName table_name = table_def.TableName;

				// Create table indices
				table_indices = new MultiVersionTableIndices(System,
															 table_name, table_def.ColumnCount);
				// The column rid list cache
				// column_rid_list = new RIDList[table_def.ColumnCount];

				// Setup the DataIndexSetDef
				SetupDataIndexSetDef();
			}
		}

		/// <summary>
		/// Loads the internal variables.
		/// </summary>
		protected void LoadInternal() {
			lock (this) {
				// Set up the stat keys.
				String table_name = table_def.Name;
				String schema_name = table_def.Schema;
				String n = table_name;
				if (schema_name.Length > 0) {
					n = schema_name + "." + table_name;
				}
				root_lock_key = "MasterTableDataSource.RootLocks." + n;
				total_hits_key = "MasterTableDataSource.Hits.Total." + n;
				file_hits_key = "MasterTableDataSource.Hits.File." + n;
				delete_hits_key = "MasterTableDataSource.Hits.Delete." + n;
				insert_hits_key = "MasterTableDataSource.Hits.Insert." + n;

				column_count = table_def.ColumnCount;

				is_closed = false;
			}
		}

		/// <summary>
		/// Returns true if this table source is closed.
		/// </summary>
		internal bool IsClosed {
			get {
				lock (this) {
					return is_closed;
				}
			}
		}

		/// <summary>
		/// Returns true if the source is read only.
		/// </summary>
		internal bool IsReadOnly {
			get { return system.ReadOnlyAccess; }
		}

		/// <summary>
		/// Returns the IStoreSystem object used to manage stores in the 
		/// persistence system.
		/// </summary>
		protected IStoreSystem StoreSystem {
			get { return store_system; }
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
		internal int AddRow(RowData data) {
			int row_number;

			lock (this) {

				row_number = InternalAddRow(data);

			} // lock

			// Update stats
			System.Stats.Increment(insert_hits_key);

			// Return the record index of the new data in the table
			return row_number;
		}

		/// <summary>
		/// Actually deletes the row from the table.
		/// </summary>
		/// <param name="row_index"></param>
		/// <remarks>
		/// This is a permanent removal of the row from the table. After this 
		/// method is called, the row can not be retrieved again. This is 
		/// generally only used by the row garbage collector.
		/// <para>
		/// There is no checking in this method.
		/// </para>
		/// </remarks>
		private void DoHardRowRemove(int row_index) {
			lock (this) {
				// If we have a rid_list for any of the columns, then update the indexing
				// there,
				for (int i = 0; i < column_count; ++i) {
					/*
					RIDList rid_list = column_rid_list[i];
					if (rid_list != null) {
						rid_list.RemoveRID(row_index);
					}
					*/
				}

				// Internally delete the row,
				InternalDeleteRow(row_index);

				// Update stats
				system.Stats.Increment(delete_hits_key);

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
				if (!IsRootLocked) {
					//      int type_key = data_store.ReadRecordType(record_index + 1);
					int type_key = ReadRecordType(record_index);
					// Check this record is marked as committed removed.
					if ((type_key & 0x0F0) == 0x020) {
						DoHardRowRemove(record_index);
					} else {
						throw new ApplicationException(
							"Row isn't marked as committed removed: " + record_index);
					}
				} else {
					throw new ApplicationException("Assertion failed: " +
									"Can't remove row, table is under a root Lock.");
				}
			}
		}

		/// <summary>
		/// Checks the given record index, and if it's possible to reclaim it 
		/// then it does so here.
		/// </summary>
		/// <param name="record_index"></param>
		/// <remarks>
		/// Rows are only removed if they are marked as committed removed.
		/// </remarks>
		/// <returns></returns>
		internal bool HardCheckAndReclaimRow(int record_index) {
			lock (this) {
				// ASSERTION: We are not under a root Lock.
				if (!IsRootLocked) {
					// Row already deleted?
					if (!RecordDeleted(record_index)) {
						int type_key = ReadRecordType(record_index);
						// Check this record is marked as committed removed.
						if ((type_key & 0x0F0) == 0x020) {
							//          Console.Out.WriteLine("[" + getName() + "] " +
							//                             "Hard Removing: " + record_index);
							DoHardRowRemove(record_index);
							return true;
						}
					}
					return false;
				} else {
					throw new ApplicationException("Assertion failed: " +
									"Can't remove row, table is under a root Lock.");
				}
			}
		}

		/// <summary>
		/// Returns the record type of the given record index.
		/// </summary>
		/// <param name="record_index"></param>
		/// <returns>
		/// Returns a type that is compatible with RawDiagnosticTable record 
		/// type.
		/// </returns>
		internal RecordState recordTypeInfo(int record_index) {
			lock (this) {
				//    ++record_index;
				if (RecordDeleted(record_index)) {
					return RecordState.Deleted;
				}
				int type_key = ReadRecordType(record_index) & 0x0F0;
				if (type_key == 0) {
					return RecordState.Uncommitted;
				} else if (type_key == 0x010) {
					return RecordState.CommittedAdded;
				} else if (type_key == 0x020) {
					return RecordState.CommittedRemoved;
				}
				return RecordState.Error;

			}
		}

		/// <summary>
		/// This is called by the 'open' method.
		/// </summary>
		/// <remarks>
		/// It performs a scan of the records and marks any rows that are 
		/// uncommitted as deleted. It also checks that the row is not within 
		/// the master index.
		/// </remarks>
		protected void DoOpeningScan() {
			lock (this) {
				DateTime in_time = DateTime.Now;

				// ASSERTION: No root locks and no pending transaction changes,
				//   VERY important we assert there's no pending transactions.
				if (IsRootLocked || HasTransactionChangesPending) {
					// This shouldn't happen if we are calling from 'open'.
					throw new Exception(
						"Odd, we are root locked or have pending journal changes.");
				}

				// This is pointless if we are in Read only mode.
				if (!IsReadOnly) {
					// A journal of index changes during this scan...
					MasterTableJournal journal = new MasterTableJournal();

					// Get the master index of rows in this table
					IIndexSet index_set = CreateIndexSet();
					IIntegerList master_index = index_set.GetIndex(0);

					// NOTE: We assume the index information is correct and that the
					//   allocation information is potentially bad.

					int row_count = RawRowCount;
					for (int i = 0; i < row_count; ++i) {
						// Is this record marked as deleted?
						if (!RecordDeleted(i)) {
							// Get the type flags for this record.
							RecordState type = recordTypeInfo(i);
							// Check if this record is marked as committed removed, or is an
							// uncommitted record.
							if (type == RecordState.CommittedRemoved ||
								type == RecordState.Uncommitted) {
								// Check it's not in the master index...
								if (!master_index.Contains(i)) {
									// Delete it.
									DoHardRowRemove(i);
								} else {
									Debug.Write(DebugLevel.Error, this,
												  "Inconsistant: Row is indexed but marked as " +
												  "removed or uncommitted.");
									Debug.Write(DebugLevel.Error, this,
												  "Row: " + i + " Type: " + type +
												  " Table: " + TableName);
									// Mark the row as committed added because it is in the index.
									WriteRecordType(i, 0x010);

								}
							} else {
								// Must be committed added.  Check it's indexed.
								if (!master_index.Contains(i)) {
									// Not indexed, so data is inconsistant.
									Debug.Write(DebugLevel.Error, this,
												  "Inconsistant: Row committed added but not in master index.");
									Debug.Write(DebugLevel.Error, this,
												  "Row: " + i + " Type: " + type +
												  " Table: " + TableName);
									// Mark the row as committed removed because it is not in the
									// index.
									WriteRecordType(i, 0x020);

								}
							}
						} else {
							// if deleted
							// Check this record isn't in the master index.
							if (master_index.Contains(i)) {
								// It's in the master index which is wrong!  We should remake the
								// indices.
								Debug.Write(DebugLevel.Error, this, "Inconsistant: Row is removed but in index.");
								Debug.Write(DebugLevel.Error, this, "Row: " + i + " Table: " + TableName);
								// Mark the row as committed added because it is in the index.
								WriteRecordType(i, 0x010);

							}
						}
					} // for (int i = 0 ; i < row_count; ++i)

					// Dispose the index set
					index_set.Dispose();

				}

				TimeSpan bench_time = DateTime.Now - in_time;
				if (Debug.IsInterestedIn(DebugLevel.Information)) {
					Debug.Write(DebugLevel.Information, this,
								  "Opening scan for " + ToString() + " (" + TableName + ") took " +
								  bench_time + "ms.");
				}

			}
		}

		/// <summary>
		/// Returns an implementation of <see cref="IRawDiagnosticTable"/> that 
		/// we can use to diagnose problems with the data in this source.
		/// </summary>
		/// <returns></returns>
		internal IRawDiagnosticTable GetRawDiagnosticTable() {
			return new MRawDiagnosticTable();
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
		internal TObject GetCellContents(int column, int row) {
			if (row < 0)
				throw new ApplicationException("'row' is < 0");
			return InternalGetCellContents(column, row);
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
		internal void AddRootLock() {
			lock (this) {
				system.Stats.Increment(root_lock_key);
				++root_lock;
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
		internal void RemoveRootLock() {
			lock (this) {
				if (!is_closed) {
					system.Stats.Decrement(root_lock_key);
					if (root_lock == 0) {
						throw new ApplicationException("Too many root locks removed!");
					}
					--root_lock;
					// If the last Lock is removed, schedule a possible collection.
					if (root_lock == 0) {
						checkForCleanup();
					}
				}
			}
		}

		/// <summary>
		/// Returns true if the table is currently under a root lock (has 1 
		/// or more root locks on it).
		/// </summary>
		internal bool IsRootLocked {
			get {
				lock (this) {
					return root_lock > 0;
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
				root_lock = 0;
			}
		}

		/// <summary>
		/// Checks to determine if it is safe to clean up any resources in the
		/// table, and if it is safe to do so, the space is reclaimed.
		/// </summary>
		internal abstract void checkForCleanup();


		internal string TransactionChangeString {
			get {
				lock (this) {
					return table_indices.TransactionChangeString;
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
					return table_indices.HasTransactionChangesPending;
				}
			}
		}


		// ---------- Inner classes ----------

		/// <summary>
		/// A <see cref="IRawDiagnosticTable"/> implementation that provides 
		/// direct access to the root data of this table source bypassing any 
		/// indexing schemes.
		/// </summary>
		/// <remarks>
		/// This interface allows for the inspection and repair of data files.
		/// </remarks>
		private sealed class MRawDiagnosticTable : IRawDiagnosticTable {
			private readonly MasterTableDataSource mtds;

			// ---------- Implemented from IRawDiagnosticTable -----------

			public int PhysicalRecordCount {
				get {
					try {
						return mtds.RawRowCount;
					} catch (IOException e) {
						throw new ApplicationException(e.Message);
					}
				}
			}

			public DataTableDef DataTableDef {
				get { return mtds.DataTableDef; }
			}

			public RecordState GetRecordState(int record_index) {
				try {
					return mtds.recordTypeInfo(record_index);
				} catch (IOException e) {
					throw new ApplicationException(e.Message);
				}
			}

			public int GetRecordSize(int record_index) {
				return -1;
			}

			public TObject GetCellContents(int column, int record_index) {
				return mtds.GetCellContents(column, record_index);
			}

			public String GetRecordMiscInformation(int record_index) {
				return null;
			}

		}

		/// <summary>
		/// A <see cref="IMutableTableDataSource"/> object as returned by the 
		/// <see cref="MasterTableDataSource.CreateTableDataSourceAtCommit(SimpleTransaction,MasterTableJournal)"/> 
		/// method.
		/// </summary>
		/// <remarks>
		/// <b>Note</b> This object is <b>not</b> thread-safe and it is assumed
		/// any use of this object will be thread exclusive. This is okay because 
		/// multiple instances of this object can be created on the same 
		/// <see cref="MasterTableDataSource"/> if multi-thread access to a 
		/// <see cref="MasterTableDataSource"/> is desirable.
		/// </remarks>
		private sealed class MMutableTableDataSource : IMutableTableDataSource {
			private readonly MasterTableDataSource mtds;

			/// <summary>
			///The Transaction object that this IMutableTableDataSource was
			/// generated from.
			/// </summary>
			/// <remarks>
			/// This reference should be used only to query database constraint 
			/// information.
			/// </remarks>
			private SimpleTransaction transaction;

			/// <summary>
			/// True if the transaction is Read-only.
			/// </summary>
			private readonly bool tran_read_only;

			/// <summary>
			/// The name of this table.
			/// </summary>
			private TableName table_name;

			/// <summary>
			/// The 'recovery point' to which the row index in this table source 
			/// has rebuilt to.
			/// </summary>
			private int row_list_rebuild;

			/// <summary>
			/// The index that represents the rows that are within this
			/// table data source within this transaction.
			/// </summary>
			private IIntegerList row_list;

			/// <summary>
			/// The 'recovery point' to which the schemes in this table source have
			/// rebuilt to.
			/// </summary>
			private int[] scheme_rebuilds;

			/// <summary>
			/// The IIndexSet for this mutable table source.
			/// </summary>
			private IIndexSet index_set;

			/// <summary>
			/// The SelectableScheme array that represents the schemes for the
			/// columns within this transaction.
			/// </summary>
			private SelectableScheme[] column_schemes;

			/// <summary>
			/// A journal of changes to this source since it was created.
			/// </summary>
			private MasterTableJournal table_journal;

			/// <summary>
			/// The last time any changes to the journal were check for referential
			/// integrity violations.
			/// </summary>
			private int last_entry_ri_check;

			public MMutableTableDataSource(MasterTableDataSource mtds, SimpleTransaction transaction,
										   MasterTableJournal journal) {
				this.mtds = mtds;
				this.transaction = transaction;
				this.index_set =
						transaction.GetIndexSetForTable(mtds);
				int col_count = DataTableDef.ColumnCount;
				this.table_name = DataTableDef.TableName;
				this.tran_read_only = transaction.IsReadOnly;
				row_list_rebuild = 0;
				scheme_rebuilds = new int[col_count];
				column_schemes = new SelectableScheme[col_count];
				table_journal = journal;
				last_entry_ri_check = table_journal.EntriesCount;
			}

			/// <summary>
			/// Executes an update referential action.
			/// </summary>
			/// <param name="constraint"></param>
			/// <param name="original_key"></param>
			/// <param name="new_key"></param>
			/// <param name="context"></param>
			/// <exception cref="ApplicationException">
			/// If the update action is "NO ACTION", and the constraint is 
			/// <see cref="ConstraintDeferrability.INITIALLY_IMMEDIATE"/>, and 
			/// the new key doesn't exist in the referral table.
			/// </exception>
			private void ExecuteUpdateReferentialAction(
									  Transaction.ColumnGroupReference constraint,
									  TObject[] original_key, TObject[] new_key,
									  IQueryContext context) {

				ConstraintAction update_rule = constraint.update_rule;
				if (update_rule == ConstraintAction.NO_ACTION &&
					constraint.deferred != ConstraintDeferrability.INITIALLY_IMMEDIATE) {
					// Constraint check is deferred
					return;
				}

				// So either update rule is not NO ACTION, or if it is we are initially
				// immediate.
				IMutableTableDataSource key_table =
										 transaction.GetTable(constraint.key_table_name);
				DataTableDef table_def = key_table.DataTableDef;
				int[] key_cols = TableDataConglomerate.FindColumnIndices(
													  table_def, constraint.key_columns);
				IntegerVector key_entries =
					   TableDataConglomerate.FindKeys(key_table, key_cols, original_key);

				// Are there keys effected?
				if (key_entries.Count > 0) {
					if (update_rule == ConstraintAction.NO_ACTION) {
						// Throw an exception;
						throw new DatabaseConstraintViolationException(
							DatabaseConstraintViolationException.ForeignKeyViolation,
							TableDataConglomerate.DeferredString(constraint.deferred) +
							" foreign key constraint violation on update (" +
							constraint.name + ") Columns = " +
							constraint.key_table_name.ToString() + "( " +
							TableDataConglomerate.StringColumnList(constraint.key_columns) +
							" ) -> " + constraint.ref_table_name.ToString() + "( " +
							TableDataConglomerate.StringColumnList(constraint.ref_columns) +
							" )");
					} else {
						// Perform a referential action on each updated key
						int sz = key_entries.Count;
						for (int i = 0; i < sz; ++i) {
							int row_index = key_entries[i];
							RowData row_data = new RowData(key_table);
							row_data.SetFromRow(row_index);
							if (update_rule == ConstraintAction.CASCADE) {
								// Update the keys
								for (int n = 0; n < key_cols.Length; ++n) {
									row_data.SetColumnData(key_cols[n], new_key[n]);
								}
								key_table.UpdateRow(row_index, row_data);
							} else if (update_rule == ConstraintAction.SET_NULL) {
								for (int n = 0; n < key_cols.Length; ++n) {
									row_data.SetToNull(key_cols[n]);
								}
								key_table.UpdateRow(row_index, row_data);
							} else if (update_rule == ConstraintAction.SET_DEFAULT) {
								for (int n = 0; n < key_cols.Length; ++n) {
									row_data.SetToDefault(key_cols[n], context);
								}
								key_table.UpdateRow(row_index, row_data);
							} else {
								throw new Exception("Do not understand referential action: " + update_rule);
							}
						}
						// Check referential integrity of modified table,
						key_table.ConstraintIntegrityCheck();
					}
				}
			}

			/// <summary>
			/// Executes a delete referential action.
			/// </summary>
			/// <param name="constraint"></param>
			/// <param name="original_key"></param>
			/// <param name="context"></param>
			/// <exception cref="ApplicationException">
			/// If the delete action is "NO ACTION", and the constraint is 
			/// <see cref="ConstraintDeferrability.INITIALLY_IMMEDIATE"/>, and 
			/// the new key doesn't exist in the referral table.
			/// </exception>
			private void ExecuteDeleteReferentialAction(
									  Transaction.ColumnGroupReference constraint,
									  TObject[] original_key, IQueryContext context) {

				ConstraintAction delete_rule = constraint.delete_rule;
				if (delete_rule == ConstraintAction.NO_ACTION &&
					constraint.deferred != ConstraintDeferrability.INITIALLY_IMMEDIATE) {
					// Constraint check is deferred
					return;
				}

				// So either delete rule is not NO ACTION, or if it is we are initially
				// immediate.
				IMutableTableDataSource key_table =
										 transaction.GetTable(constraint.key_table_name);
				DataTableDef table_def = key_table.DataTableDef;
				int[] key_cols = TableDataConglomerate.FindColumnIndices(
													  table_def, constraint.key_columns);
				IntegerVector key_entries =
					   TableDataConglomerate.FindKeys(key_table, key_cols, original_key);

				// Are there keys effected?
				if (key_entries.Count > 0) {
					if (delete_rule == ConstraintAction.NO_ACTION) {
						// Throw an exception;
						throw new DatabaseConstraintViolationException(
							DatabaseConstraintViolationException.ForeignKeyViolation,
							TableDataConglomerate.DeferredString(constraint.deferred) +
							" foreign key constraint violation on delete (" +
							constraint.name + ") Columns = " +
							constraint.key_table_name.ToString() + "( " +
							TableDataConglomerate.StringColumnList(constraint.key_columns) +
							" ) -> " + constraint.ref_table_name.ToString() + "( " +
							TableDataConglomerate.StringColumnList(constraint.ref_columns) +
							" )");
					} else {
						// Perform a referential action on each updated key
						int sz = key_entries.Count;
						for (int i = 0; i < sz; ++i) {
							int row_index = key_entries[i];
							RowData row_data = new RowData(key_table);
							row_data.SetFromRow(row_index);
							if (delete_rule == ConstraintAction.CASCADE) {
								// Cascade the removal of the referenced rows
								key_table.RemoveRow(row_index);
							} else if (delete_rule == ConstraintAction.SET_NULL) {
								for (int n = 0; n < key_cols.Length; ++n) {
									row_data.SetToNull(key_cols[n]);
								}
								key_table.UpdateRow(row_index, row_data);
							} else if (delete_rule == ConstraintAction.SET_DEFAULT) {
								for (int n = 0; n < key_cols.Length; ++n) {
									row_data.SetToDefault(key_cols[n], context);
								}
								key_table.UpdateRow(row_index, row_data);
							} else {
								throw new Exception("Do not understand referential action: " + delete_rule);
							}
						}
						// Check referential integrity of modified table,
						key_table.ConstraintIntegrityCheck();
					}
				}
			}

			/// <summary>
			/// Returns the entire row list for this table.
			/// </summary>
			/// <remarks>
			/// This will request this information from the master source.
			/// </remarks>
			private IIntegerList RowIndexList {
				get {
					if (row_list == null) {
						row_list = index_set.GetIndex(0);
					}
					return row_list;
				}
			}

			/// <summary>
			/// Ensures that the row list is as current as the latest journal change.
			/// </summary>
			/// <remarks>
			/// We can be assured that when this is called, no journal changes 
			/// will occur concurrently. However we still need to synchronize 
			/// because multiple reads are valid.
			/// </remarks>
			private void EnsureRowIndexListCurrent() {
				int rebuild_index = row_list_rebuild;
				int journal_count = table_journal.EntriesCount;
				while (rebuild_index < journal_count) {
					byte command = table_journal.GetCommand(rebuild_index);
					int row_index = table_journal.GetRowIndex(rebuild_index);
					if (MasterTableJournal.IsAddCommand(command)) {
						// Add to 'row_list'.
						bool b = RowIndexList.UniqueInsertSort(row_index);
						if (b == false) {
							throw new ApplicationException(
								  "Row index already used in this table (" + row_index + ")");
						}
					} else if (MasterTableJournal.IsRemoveCommand(command)) {
						// Remove from 'row_list'
						bool b = RowIndexList.RemoveSort(row_index);
						if (b == false) {
							throw new ApplicationException("Row index removed that wasn't in this table!");
						}
					} else {
						throw new ApplicationException("Unrecognised journal command.");
					}
					++rebuild_index;
				}
				// It's now current (row_list_rebuild == journal_count);
				row_list_rebuild = rebuild_index;
			}

			/// <summary>
			/// Ensures that the scheme column index is as current as the latest
			/// journal change.
			/// </summary>
			/// <param name="column"></param>
			private void EnsureColumnSchemeCurrent(int column) {
				SelectableScheme scheme = column_schemes[column];
				// NOTE: We should be assured that no Write operations can occur over
				//   this section of code because writes are exclusive operations
				//   within a transaction.
				// Are there journal entries pending on this scheme since?
				int rebuild_index = scheme_rebuilds[column];
				int journal_count = table_journal.EntriesCount;
				while (rebuild_index < journal_count) {
					byte command = table_journal.GetCommand(rebuild_index);
					int row_index = table_journal.GetRowIndex(rebuild_index);
					if (MasterTableJournal.IsAddCommand(command)) {
						scheme.Insert(row_index);
					} else if (MasterTableJournal.IsRemoveCommand(command)) {
						scheme.Remove(row_index);
					} else {
						throw new ApplicationException("Unrecognised journal command.");
					}
					++rebuild_index;
				}
				scheme_rebuilds[column] = rebuild_index;
			}

			// ---------- Implemented from IMutableTableDataSource ----------

			public TransactionSystem System {
				get { return mtds.System; }
			}

			public DataTableDef DataTableDef {
				get { return mtds.DataTableDef; }
			}

			public int RowCount {
				get {
					// Ensure the row list is up to date.
					EnsureRowIndexListCurrent();
					return RowIndexList.Count;
				}
			}

			public IRowEnumerator GetRowEnumerator() {
				// Ensure the row list is up to date.
				EnsureRowIndexListCurrent();
				// Get an iterator across the row list.
				IIntegerIterator iterator = RowIndexList.GetIterator();
				// Wrap it around a IRowEnumerator object.
				return new RowEnumerationImpl(iterator);
			}

			private class RowEnumerationImpl : IRowEnumerator {
				public RowEnumerationImpl(IIntegerIterator iterator) {
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
			public TObject GetCellContents(int column, int row) {
				return mtds.GetCellContents(column, row);
			}

			// NOTE: Returns an immutable version of the scheme...
			public SelectableScheme GetColumnScheme(int column) {
				SelectableScheme scheme = column_schemes[column];
				// Cache the scheme in this object.
				if (scheme == null) {
					scheme = mtds.CreateSelectableSchemeForColumn(index_set, this, column);
					column_schemes[column] = scheme;
				}

				// Update the underlying scheme to the most current version.
				EnsureColumnSchemeCurrent(column);

				return scheme;
			}

			// ---------- Table Modification ----------

			public int AddRow(RowData row_data) {

				// Check the transaction isn't Read only.
				if (tran_read_only) {
					throw new Exception("Transaction is Read only.");
				}

				// Check this isn't a Read only source
				if (mtds.IsReadOnly) {
					throw new ApplicationException("Can not add row - table is Read only.");
				}

				// Add to the master.
				int row_index;
				try {
					row_index = mtds.AddRow(row_data);
				} catch (IOException e) {
					mtds.Debug.WriteException(e);
					throw new ApplicationException("IO Error: " + e.Message);
				}

				// Note this doesn't need to be synchronized because we are exclusive on
				// this table.
				// Add this change to the table journal.
				table_journal.AddEntry(JournalCommand.TABLE_ADD, row_index);

				return row_index;
			}

			public void RemoveRow(int row_index) {

				// Check the transaction isn't Read only.
				if (tran_read_only) {
					throw new Exception("Transaction is Read only.");
				}

				// Check this isn't a Read only source
				if (mtds.IsReadOnly) {
					throw new ApplicationException("Can not remove row - table is Read only.");
				}

				// NOTE: This must <b>NOT</b> call 'RemoveRow' in MasterTableDataSource.
				//   We do not want to delete a row permanently from the underlying
				//   file because the transaction using this data source may yet decide
				//   to roll back the change and not delete the row.

				// Note this doesn't need to be synchronized because we are exclusive on
				// this table.
				// Add this change to the table journal.
				table_journal.AddEntry(JournalCommand.TABLE_REMOVE, row_index);

			}

			public int UpdateRow(int row_index, RowData row_data) {

				// Check the transaction isn't Read only.
				if (tran_read_only) {
					throw new Exception("Transaction is Read only.");
				}

				// Check this isn't a Read only source
				if (mtds.IsReadOnly) {
					throw new ApplicationException("Can not update row - table is Read only.");
				}

				// Note this doesn't need to be synchronized because we are exclusive on
				// this table.
				// Add this change to the table journal.
				table_journal.AddEntry(JournalCommand.TABLE_UPDATE_REMOVE, row_index);

				// Add to the master.
				int new_row_index;
				try {
					new_row_index = mtds.AddRow(row_data);
				} catch (IOException e) {
					mtds.Debug.WriteException(e);
					throw new ApplicationException("IO Error: " + e.Message);
				}

				// Note this doesn't need to be synchronized because we are exclusive on
				// this table.
				// Add this change to the table journal.
				table_journal.AddEntry(JournalCommand.TABLE_UPDATE_ADD, new_row_index);

				return new_row_index;
			}


			public void FlushIndexChanges() {
				EnsureRowIndexListCurrent();
				// This will flush all of the column schemes
				for (int i = 0; i < column_schemes.Length; ++i) {
					GetColumnScheme(i);
				}
			}

			public void ConstraintIntegrityCheck() {
				try {

					// Early exit condition
					if (last_entry_ri_check == table_journal.EntriesCount) {
						return;
					}

					// This table name
					DataTableDef table_def = DataTableDef;
					TableName table_name = table_def.TableName;
					IQueryContext context =
							   new SystemQueryContext(transaction, table_name.Schema);

					// Are there any added, deleted or updated entries in the journal since
					// we last checked?
					IntegerVector rows_updated = new IntegerVector();
					IntegerVector rows_deleted = new IntegerVector();
					IntegerVector rows_added = new IntegerVector();

					int size = table_journal.EntriesCount;
					for (int i = last_entry_ri_check; i < size; ++i) {
						byte tc = table_journal.GetCommand(i);
						int row_index = table_journal.GetRowIndex(i);
						if (tc == JournalCommand.TABLE_REMOVE ||
							tc == JournalCommand.TABLE_UPDATE_REMOVE) {
							rows_deleted.AddInt(row_index);
							// If this is in the rows_added list, remove it from rows_added
							int ra_i = rows_added.IndexOf(row_index);
							if (ra_i != -1) {
								rows_added.RemoveIntAt(ra_i);
							}
						} else if (tc == JournalCommand.TABLE_ADD ||
								 tc == JournalCommand.TABLE_UPDATE_ADD) {
							rows_added.AddInt(row_index);
						}

						if (tc == JournalCommand.TABLE_UPDATE_REMOVE) {
							rows_updated.AddInt(row_index);
						} else if (tc == JournalCommand.TABLE_UPDATE_ADD) {
							rows_updated.AddInt(row_index);
						}
					}

					// Were there any updates or deletes?
					if (rows_deleted.Count > 0) {
						// Get all references on this table
						Transaction.ColumnGroupReference[] foreign_constraints =
							 Transaction.QueryTableImportedForeignKeyReferences(transaction,
																				table_name);

						// For each foreign constraint
						for (int n = 0; n < foreign_constraints.Length; ++n) {
							Transaction.ColumnGroupReference constraint =
																	   foreign_constraints[n];
							// For each deleted/updated record in the table,
							for (int i = 0; i < rows_deleted.Count; ++i) {
								int row_index = rows_deleted[i];
								// What was the key before it was updated/deleted
								int[] cols = TableDataConglomerate.FindColumnIndices(
															  table_def, constraint.ref_columns);
								TObject[] original_key = new TObject[cols.Length];
								int null_count = 0;
								for (int p = 0; p < cols.Length; ++p) {
									original_key[p] = GetCellContents(cols[p], row_index);
									if (original_key[p].IsNull) {
										++null_count;
									}
								}
								// Check the original key isn't null
								if (null_count != cols.Length) {
									// Is is an update?
									int update_index = rows_updated.IndexOf(row_index);
									if (update_index != -1) {
										// Yes, this is an update
										int row_index_add = rows_updated[update_index + 1];
										// It must be an update, so first see if the change caused any
										// of the keys to change.
										bool key_changed = false;
										TObject[] key_updated_to = new TObject[cols.Length];
										for (int p = 0; p < cols.Length; ++p) {
											key_updated_to[p] = GetCellContents(cols[p], row_index_add);
											if (original_key[p].CompareTo(key_updated_to[p]) != 0) {
												key_changed = true;
											}
										}
										if (key_changed) {
											// Allow the delete, and execute the action,
											// What did the key update to?
											ExecuteUpdateReferentialAction(constraint,
																original_key, key_updated_to, context);
										}
										// If the key didn't change, we don't need to do anything.
									} else {
										// No, so it must be a delete,
										// This will look at the referencee table and if it contains
										// the key, work out what to do with it.
										ExecuteDeleteReferentialAction(constraint, original_key,
																	   context);
									}

								}  // If the key isn't null

							}  // for each deleted rows

						}  // for each foreign key reference to this table

					}

					// Were there any rows added (that weren't deleted)?
					if (rows_added.Count > 0) {
						int[] row_indices = rows_added.ToIntArray();

						// Check for any field constraint violations in the added rows
						TableDataConglomerate.CheckFieldConstraintViolations(
															 transaction, this, row_indices);
						// Check this table, adding the given row_index, immediate
						TableDataConglomerate.CheckAddConstraintViolations(
							transaction, this,
							row_indices, ConstraintDeferrability.INITIALLY_IMMEDIATE);
					}

				} catch (DatabaseConstraintViolationException e) {

					// If a constraint violation, roll back the changes since the last
					// check.
					int rollback_point = table_journal.EntriesCount - last_entry_ri_check;
					if (row_list_rebuild <= rollback_point) {
						table_journal.RollbackEntries(rollback_point);
					} else {
						Console.Out.WriteLine(
						   "Warning: rebuild_pointer is after rollback point so we can't " +
						   "rollback to the point before the constraint violation.");
					}

					throw e;

				} finally {
					// Make sure we update the 'last_entry_ri_check' variable
					last_entry_ri_check = table_journal.EntriesCount;
				}

			}

			public MasterTableJournal Journal {
				get { return table_journal; }
			}

			public void Dispose() {
				// Dispose and invalidate the schemes
				// This is really a safety measure to ensure the schemes can't be
				// used outside the scope of the lifetime of this object.
				for (int i = 0; i < column_schemes.Length; ++i) {
					SelectableScheme scheme = column_schemes[i];
					if (scheme != null) {
						scheme.Dispose();
						column_schemes[i] = null;
					}
				}
				row_list = null;
				table_journal = null;
				scheme_rebuilds = null;
				index_set = null;
				transaction = null;
			}

			public void AddRootLock() {
				mtds.AddRootLock();
			}

			public void RemoveRootLock() {
				mtds.RemoveRootLock();
			}

		}

	}
}