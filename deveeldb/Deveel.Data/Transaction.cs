//  
//  Transaction.cs
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
using System.Collections;
using System.IO;

using Deveel.Data.Collections;
using Deveel.Diagnostics;
using Deveel.Math;

namespace Deveel.Data {
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
	public class Transaction : SimpleTransaction, IDisposable {

		// ---------- Member variables ----------

		/// <summary>
		/// The TableDataConglomerate that this transaction is within the context of.
		/// </summary>
		private TableDataConglomerate conglomerate;

		/// <summary>
		/// The commit_id that represents the id of the last commit that occurred
		/// when this transaction was created.
		/// </summary>
		private readonly long commit_id;

		/// <summary>
		///  All tables touched by this transaction.  (IMutableTableDataSource)
		/// </summary>
		private ArrayList touched_tables;

		/// <summary>
		/// All tables selected from in this transaction.  (MasterTableDataSource)
		/// </summary>
		private readonly ArrayList selected_from_tables;

		/// <summary>
		/// The name of all database objects that were created in this transaction.
		/// This is used for a namespace collision test during commit.
		/// </summary>
		private readonly ArrayList created_database_objects;

		/// <summary>
		/// The name of all database objects that were dropped in this transaction.
		/// This is used for a namespace collision test during commit.
		/// </summary>
		private readonly ArrayList dropped_database_objects;

		private Hashtable cursors;

		/// <summary>
		/// The journal for this transaction.  This journal describes all changes
		/// made to the database by this transaction.
		/// </summary>
		private TransactionJournal journal;

		/// <summary>
		/// The list of IInternalTableInfo objects that are containers for generating
		/// internal tables (GTDataSource).
		/// </summary>
		private readonly IInternalTableInfo[] internal_tables;

		/// <summary>
		/// A pointer in the internal_tables list.
		/// </summary>
		private int internal_tables_i;

		/// <summary>
		/// True if an error should be generated on a dirty select.
		/// </summary>
		private bool transaction_error_on_dirty_select;

		/// <summary>
		/// True if this transaction is closed.
		/// </summary>
		private bool closed;


		internal Transaction(TableDataConglomerate conglomerate,
					long commit_id, ArrayList visible_tables,
					ArrayList table_indices)

			: base(conglomerate.System, conglomerate.SequenceManager) {

			this.conglomerate = conglomerate;
			this.commit_id = commit_id;
			this.closed = false;

			this.created_database_objects = new ArrayList();
			this.dropped_database_objects = new ArrayList();

			this.touched_tables = new ArrayList();
			this.selected_from_tables = new ArrayList();
			journal = new TransactionJournal();

			cursors = new Hashtable();

			// Set up all the visible tables
			int sz = visible_tables.Count;
			for (int i = 0; i < sz; ++i) {
				AddVisibleTable((MasterTableDataSource)visible_tables[i],
								(IIndexSet)table_indices[i]);
			}

			// NOTE: We currently only support 8 - internal tables to the transaction
			//  layer, and internal tables to the database connection layer.
			internal_tables = new IInternalTableInfo[8];
			internal_tables_i = 0;
			AddInternalTableInfo(new TransactionInternalTables(this));

			System.Stats.Increment("Transaction.count");

			// Defaults to true (should be changed by called 'setErrorOnDirtySelect'
			// method.
			transaction_error_on_dirty_select = true;
		}

		/// <summary>
		/// Returns the TableDataConglomerate of this transaction.
		/// </summary>
		internal TableDataConglomerate Conglomerate {
			get { return conglomerate; }
		}

		/// <summary>
		/// Adds an internal table container (InternalTableInfo) used to
		/// resolve internal tables.
		/// </summary>
		/// <param name="info"></param>
		/// <remarks>
		/// This is intended as a way for the <see cref="DatabaseConnection"/> 
		/// layer to plug in <i>virtual</i> tables, such as those showing 
		/// connection statistics, etc. It also allows modelling database objects 
		/// as tables, such as sequences, triggers, procedures, etc.
		/// </remarks>
		internal void AddInternalTableInfo(IInternalTableInfo info) {
			if (internal_tables_i >= internal_tables.Length) {
				throw new Exception("Internal table list bounds reached.");
			}
			internal_tables[internal_tables_i] = info;
			++internal_tables_i;
		}

		/// <summary>
		/// Returns the 'commit_id' which is the last commit that occured 
		/// before this transaction was created.
		/// </summary>
		/// <remarks>
		/// <b>Note</b> Don't make this synchronized over anything. This is 
		/// accessed by <see cref="OpenTransactionList"/>.
		/// </remarks>
		internal long CommitID {
			get {
				// REINFORCED NOTE: This absolutely must never be synchronized because
				//   it is accessed by OpenTransactionList synchronized.
				return commit_id;
			}
		}

		// ----- Operations within the context of this transaction -----

		/// <inheritdoc/>
		internal override IMutableTableDataSource CreateMutableTableDataSourceAtCommit(MasterTableDataSource master) {
			// Create the table for this transaction.
			IMutableTableDataSource table = master.CreateTableDataSourceAtCommit(this);
			// Log in the journal that this table was touched by the transaction.
			journal.EntryAddTouchedTable(master.TableID);
			touched_tables.Add(table);
			return table;
		}

		/// <summary>
		/// Called by the query evaluation layer when information is selected
		/// from this table as part of this transaction.
		/// </summary>
		/// <param name="table_name"></param>
		/// <remarks>
		/// When there is a select query on a table, when the transaction is 
		/// committed we should look for any concurrently committed changes 
		/// to the table.  If there are any, then any selects on the table 
		/// should be considered incorrect and cause a commit failure.
		/// </remarks>
		public void AddSelectedFromTable(TableName table_name) {
			// Special handling of internal tables,
			if (IsDynamicTable(table_name)) {
				return;
			}

			MasterTableDataSource master = FindVisibleTable(table_name, false);
			if (master == null) {
				throw new StatementException(
									  "Table with name not available: " + table_name);
			}
			//    Console.Out.WriteLine("Selected from table: " + table_name);
			lock (selected_from_tables) {
				if (!selected_from_tables.Contains(master)) {
					selected_from_tables.Add(master);
				}
			}

		}



		/// <summary>
		/// Copies all the tables within this transaction view to the 
		/// destination conglomerate object.
		/// </summary>
		/// <param name="dest_conglomerate"></param>
		/// <remarks>
		/// Some care should be taken with security when using this method. 
		/// This is useful for generating a backup of the current view of the 
		/// database that can work without interfering with the general
		/// operation of the database.
		/// </remarks>
		internal void liveCopyAllDataTo(TableDataConglomerate dest_conglomerate) {
			// Create a new TableDataConglomerate using the same settings from this
			// TransactionSystem but on the new IStoreSystem.
			int sz = VisibleTableCount;

			// The list to copy (in the order to copy in).
			// We WriteByte the 'SEQUENCE_INFO' at the very end of the table list to copy.
			ArrayList copy_list = new ArrayList(sz);

			MasterTableDataSource last_entry = null;
			for (int i = 0; i < sz; ++i) {
				MasterTableDataSource master_table = GetVisibleTable(i);
				TableName table_name = master_table.DataTableDef.TableName;
				if (table_name.Equals(TableDataConglomerate.SYS_SEQUENCE_INFO)) {
					last_entry = master_table;
				} else {
					copy_list.Add(master_table);
				}
			}
			copy_list.Insert(0, last_entry);

			try {
				// For each master table,
				for (int i = 0; i < sz; ++i) {

					MasterTableDataSource master_table =
										   (MasterTableDataSource)copy_list[i];
					TableName table_name = master_table.DataTableDef.TableName;

					// Create a destination transaction
					Transaction dest_transaction = dest_conglomerate.CreateTransaction();

					// The view of this table within this transaction.
					IIndexSet index_set = GetIndexSetForTable(master_table);

					// If the table already exists then drop it
					if (dest_transaction.TableExists(table_name)) {
						dest_transaction.DropTable(table_name);
					}

					// Copy it into the destination conglomerate.
					dest_transaction.CopyTable(master_table, index_set);

					// Close and commit the transaction in the destination conglomeration.      
					dest_transaction.Commit();

					// Dispose the IIndexSet
					index_set.Dispose();

				}

			} catch (TransactionException e) {
				Debug.WriteException(e);
				throw new Exception("Transaction Error when copying table: " +
										   e.Message);
			}
		}

		// ---------- Dynamically generated tables ----------

		/// <inheritdoc/>
		protected override bool IsDynamicTable(TableName table_name) {
			for (int i = 0; i < internal_tables.Length; ++i) {
				IInternalTableInfo info = internal_tables[i];
				if (info != null) {
					if (info.ContainsTableName(table_name)) {
						return true;
					}
				}
			}
			return false;
		}

		/// <inheritdoc/>
		protected override TableName[] GetDynamicTables() {
			int sz = 0;
			for (int i = 0; i < internal_tables.Length; ++i) {
				IInternalTableInfo info = internal_tables[i];
				if (info != null) {
					sz += info.TableCount;
				}
			}

			TableName[] list = new TableName[sz];
			int index = 0;

			for (int i = 0; i < internal_tables.Length; ++i) {
				IInternalTableInfo info = internal_tables[i];
				if (info != null) {
					sz = info.TableCount;
					for (int n = 0; n < sz; ++n) {
						list[index] = info.GetTableName(n);
						++index;
					}
				}
			}

			return list;
		}

		/// <inheritdoc/>
		protected override DataTableDef GetDynamicDataTableDef(TableName table_name) {

			for (int i = 0; i < internal_tables.Length; ++i) {
				IInternalTableInfo info = internal_tables[i];
				if (info != null) {
					int index = info.FindTableName(table_name);
					if (index != -1) {
						return info.GetDataTableDef(index);
					}
				}
			}

			throw new Exception("Not an internal table: " + table_name);
		}

		/// <inheritdoc/>
		protected override IMutableTableDataSource GetDynamicTable(TableName table_name) {

			for (int i = 0; i < internal_tables.Length; ++i) {
				IInternalTableInfo info = internal_tables[i];
				if (info != null) {
					int index = info.FindTableName(table_name);
					if (index != -1) {
						return info.CreateInternalTable(index);
					}
				}
			}

			throw new Exception("Not an internal table: " + table_name);
		}

		/// <inheritdoc/>
		protected override String GetDynamicTableType(TableName table_name) {
			// Otherwise we need to look up the table in the internal table list,
			for (int i = 0; i < internal_tables.Length; ++i) {
				IInternalTableInfo info = internal_tables[i];
				if (info != null) {
					int index = info.FindTableName(table_name);
					if (index != -1) {
						return info.GetTableType(index);
					}
				}
			}
			// No internal table found, so report the error.
			throw new Exception("No table '" + table_name +
									   "' to report type for.");
		}


		// ---------- Transaction manipulation ----------

		/// <summary>
		/// Creates a new table within this transaction with the given sector 
		/// size.
		/// </summary>
		/// <param name="table_def"></param>
		/// <param name="data_sector_size"></param>
		/// <param name="index_sector_size"></param>
		/// <remarks>
		/// This should only be called under an exclusive lock on the connection.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the table already exists.
		/// </exception>
		public void CreateTable(DataTableDef table_def, int data_sector_size, int index_sector_size) {
			TableName table_name = table_def.TableName;
			MasterTableDataSource master = FindVisibleTable(table_name, false);
			if (master != null) {
				throw new StatementException(
										 "Table '" + table_name + "' already exists.");
			}

			table_def.SetImmutable();

			if (data_sector_size < 27) {
				data_sector_size = 27;
			} else if (data_sector_size > 4096) {
				data_sector_size = 4096;
			}

			// Create the new master table and add to list of visible tables.
			master = conglomerate.CreateMasterTable(table_def, data_sector_size,
													index_sector_size);
			// Add this table (and an index set) for this table.
			AddVisibleTable(master, master.CreateIndexSet());

			// Log in the journal that this transaction touched the table_id.
			int table_id = master.TableID;
			journal.EntryAddTouchedTable(table_id);

			// Log in the journal that we created this table.
			journal.EntryTableCreate(table_id);

			// Add entry to the Sequences table for the native generator for this
			// table.
			SequenceManager.AddNativeTableGenerator(this, table_name);

			// Notify that this database object has been successfully created.
			OnDatabaseObjectCreated(table_name);

		}

		/// <summary>
		/// Creates a new table within this transaction with the given sector 
		/// size.
		/// </summary>
		/// <param name="table_def"></param>
		/// <remarks>
		/// This should only be called under an exclusive lock on the connection.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the table already exists.
		/// </exception>
		public void CreateTable(DataTableDef table_def) {
			// data sector size defaults to 251
			// index sector size defaults to 1024
			CreateTable(table_def, 251, 1024);
		}

		/// <summary>
		/// Given a DataTableInfo, if the table exists then it is updated otherwise
		/// if it doesn't exist then it is created.
		/// </summary>
		/// <param name="table_def"></param>
		/// <param name="data_sector_size"></param>
		/// <param name="index_sector_size"></param>
		/// <remarks>
		/// This should only be used as very fine grain optimization for 
		/// creating/altering tables. If in the future the underlying table 
		/// model is changed so that the given <paramref name="data_sector_size"/>
		/// and <paramref name="index_sector_size"/> values are unapplicable, 
		/// then the value will be ignored.
		/// </remarks>
		public void AlterCreateTable(DataTableDef table_def, int data_sector_size, int index_sector_size) {
			if (!TableExists(table_def.TableName)) {
				CreateTable(table_def, data_sector_size, index_sector_size);
			} else {
				AlterTable(table_def.TableName, table_def, data_sector_size, index_sector_size);
			}
		}

		/// <summary>
		/// Drops a table within this transaction.
		/// </summary>
		/// <param name="table_name"></param>
		/// <remarks>
		/// This should only be called under an exclusive lock on the connection.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the table does not exist.
		/// </exception>
		public void DropTable(TableName table_name) {
			//    Console.Out.WriteLine(this + " DROP: " + table_name);
			MasterTableDataSource master = FindVisibleTable(table_name, false);

			if (master == null) {
				throw new StatementException("Table '" + table_name + "' doesn't exist.");
			}

			// Removes this table from the visible table list of this transaction
			RemoveVisibleTable(master);

			// Log in the journal that this transaction touched the table_id.
			int table_id = master.TableID;
			journal.EntryAddTouchedTable(table_id);

			// Log in the journal that we dropped this table.
			journal.EntryTableDrop(table_id);

			// Remove the native sequence generator (in this transaction) for this
			// table.
			SequenceManager.RemoveNativeTableGenerator(this, table_name);

			// Notify that this database object has been dropped
			OnDatabaseObjectDropped(table_name);

		}

		/// <summary>
		/// Generates an exact copy of the table within this transaction.
		/// </summary>
		/// <param name="src_master_table"></param>
		/// <param name="index_set"></param>
		/// <remarks>
		/// It is recommended that the table is dropped before the copy is made. 
		/// The purpose of this method is to generate a temporary table that 
		/// can be modified without fear of another transaction changing the 
		/// contents in another transaction. This also provides a convenient 
		/// way to compact a table because any spare space is removed when the 
		/// table is copied. It also allows us to make a copy of 
		/// <see cref="MasterTableDataSource"/> into a foreign conglomerate 
		/// which allows us to implement a backup procedure.
		/// <para>
		/// This method does <b>not</b> assume the given <see cref="MasterTableDataSource"/> 
		/// is contained, or has once been contained within this conglomerate.
		/// </para>
		/// </remarks>
		internal void CopyTable(MasterTableDataSource src_master_table, IIndexSet index_set) {

			DataTableDef table_def = src_master_table.DataTableDef;
			TableName table_name = table_def.TableName;
			MasterTableDataSource master = FindVisibleTable(table_name, false);
			if (master != null) {
				throw new StatementException(
						  "Unable to copy.  Table '" + table_name + "' already exists.");
			}

			// Copy the master table and add to the list of visible tables.
			master = conglomerate.CopyMasterTable(src_master_table, index_set);
			// Add this visible table
			AddVisibleTable(master, master.CreateIndexSet());

			// Log in the journal that this transaction touched the table_id.
			int table_id = master.TableID;
			journal.EntryAddTouchedTable(table_id);

			// Log in the journal that we created this table.
			journal.EntryTableCreate(table_id);

			// Add entry to the Sequences table for the native generator for this
			// table.
			SequenceManager.AddNativeTableGenerator(this, table_name);

			// Notify that this database object has been successfully created.
			OnDatabaseObjectCreated(table_name);

		}

		/// <summary>
		/// Alter the table with the given name to the new definition and give 
		/// the copied table a new data sector size.
		/// </summary>
		/// <param name="table_name"></param>
		/// <param name="table_def"></param>
		/// <param name="data_sector_size"></param>
		/// <param name="index_sector_size"></param>
		/// <remarks>
		/// This copies all columns that were in the original table to the new
		/// altered table if the name is the same.  Any names that don't exist are
		/// set to the default value.
		/// <para>
		/// This should only be called under an exclusive lock on the connection.
		/// </para>
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the table does not exist.
		/// </exception>
		public void AlterTable(TableName table_name, DataTableDef table_def, int data_sector_size, int index_sector_size) {
			table_def.SetImmutable();

			// The current schema context is the schema of the table name
			String current_schema = table_name.Schema;
			SystemQueryContext context = new SystemQueryContext(this, current_schema);

			// Get the next unique id of the unaltered table.
			long next_id = NextUniqueID(table_name);

			// Drop the current table
			IMutableTableDataSource c_table = GetTable(table_name);
			DropTable(table_name);
			// And create the table table
			CreateTable(table_def);
			IMutableTableDataSource altered_table = GetTable(table_name);

			// Get the new MasterTableDataSource object
			MasterTableDataSource new_master_table =
												FindVisibleTable(table_name, false);
			// Set the sequence id of the table
			new_master_table.SetUniqueID(next_id);

			// Work out which columns we have to copy to where
			int[] col_map = new int[table_def.ColumnCount];
			DataTableDef orig_td = c_table.DataTableDef;
			for (int i = 0; i < col_map.Length; ++i) {
				String col_name = table_def[i].Name;
				col_map[i] = orig_td.FindColumnName(col_name);
			}

			try {
				// First move all the rows from the old table to the new table,
				// This does NOT update the indexes.
				try {
					IRowEnumerator e = c_table.GetRowEnumerator();
					while (e.MoveNext()) {
						int row_index = e.RowIndex;
						RowData row_data = new RowData(altered_table);
						for (int i = 0; i < col_map.Length; ++i) {
							int col = col_map[i];
							if (col != -1) {
								row_data.SetColumnData(i,
													   c_table.GetCellContents(col, row_index));
							}
						}
						row_data.SetToDefault(context);
						// Note we use a low level 'AddRow' method on the master table
						// here.  This does not touch the table indexes.  The indexes are
						// built later.
						int new_row_number = new_master_table.AddRow(row_data);
						// Set the record as committed added
						new_master_table.WriteRecordType(new_row_number, 0x010);
					}
				} catch (DatabaseException e) {
					Debug.WriteException(e);
					throw new Exception(e.Message);
				}

				// PENDING: We need to copy any existing index definitions that might
				//   have been set on the table being altered.

				// Rebuild the indexes in the new master table,
				new_master_table.BuildIndexes();

				// Get the snapshot index set on the new table and set it here
				SetIndexSetForTable(new_master_table, new_master_table.CreateIndexSet());

				// Flush this out of the table cache
				FlushTableCache(table_name);

				// Ensure the native sequence generator exists...
				SequenceManager.RemoveNativeTableGenerator(this, table_name);
				SequenceManager.AddNativeTableGenerator(this, table_name);

				// Notify that this database object has been successfully dropped and
				// created.
				OnDatabaseObjectDropped(table_name);
				OnDatabaseObjectCreated(table_name);

			} catch (IOException e) {
				Debug.WriteException(e);
				throw new Exception(e.Message);
			}

		}

		/// <summary>
		/// Alters the table with the given name within this transaction to the
		/// specified table definition.
		/// </summary>
		/// <param name="table_name"></param>
		/// <param name="table_def"></param>
		/// <remarks>
		/// This should only be called under an exclusive lock on the connection.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the table does not exist.
		/// </exception>
		public void AlterTable(TableName table_name, DataTableDef table_def) {

			// Make sure we remember the current sector size of the altered table so
			// we can create the new table with the original size.
			try {

				int current_data_sector_size;
				MasterTableDataSource master = FindVisibleTable(table_name, false);
				/*
				TODO:
				if (master is V1MasterTableDataSource) {
					current_data_sector_size =
								   ((V1MasterTableDataSource)master).rawDataSectorSize();
				} else {
				*/
					current_data_sector_size = -1;
				//}
				// HACK: We use index sector size of 2043 for all altered tables
				AlterTable(table_name, table_def, current_data_sector_size, 2043);

			} catch (IOException e) {
				throw new Exception("IO Error: " + e.Message);
			}

		}





		/// <summary>
		/// Checks all the rows in the table for immediate constraint violations
		/// and when the transaction is next committed check for all deferred
		/// constraint violations.
		/// </summary>
		/// <param name="table_name"></param>
		/// <remarks>
		/// This method is used when the constraints on a table changes and we
		/// need to determine if any constraint violations occurred. To the 
		/// constraint checking system, this is like adding all the rows to 
		/// the given table.
		/// </remarks>
		public void CheckAllConstraints(TableName table_name) {
			// Get the table
			ITableDataSource table = GetTable(table_name);
			// Get all the rows in the table
			int[] rows = new int[table.RowCount];
			IRowEnumerator row_enum = table.GetRowEnumerator();
			int i = 0;
			while (row_enum.MoveNext()) {
				rows[i] = row_enum.RowIndex;
				++i;
			}
			// Check the constraints of all the rows in the table.
			TableDataConglomerate.CheckAddConstraintViolations(
							 this, table, rows, ConstraintDeferrability.INITIALLY_IMMEDIATE);

			// Add that we altered this table in the journal
			MasterTableDataSource master = FindVisibleTable(table_name, false);
			if (master == null) {
				throw new StatementException(
										  "Table '" + table_name + "' doesn't exist.");
			}

			// Log in the journal that this transaction touched the table_id.
			int table_id = master.TableID;

			journal.EntryAddTouchedTable(table_id);
			// Log in the journal that we dropped this table.
			journal.EntryTableConstraintAlter(table_id);

		}

		/// <summary>
		///  Compacts the table with the given name within this transaction.
		/// </summary>
		/// <param name="table_name"></param>
		/// <exception cref="StatementException">
		/// If the table doesn't exist.
		/// </exception>
		public void CompactTable(TableName table_name) {

			// Find the master table.
			MasterTableDataSource current_table = FindVisibleTable(table_name, false);
			if (current_table == null) {
				throw new StatementException(
										   "Table '" + table_name + "' doesn't exist.");
			}

			// If the table is worth compacting, or the table is a
			// V1MasterTableDataSource
			if (current_table.Compact) {
				// The view of this table within this transaction.
				IIndexSet index_set = GetIndexSetForTable(current_table);
				// Drop the current table
				DropTable(table_name);
				// And copy to the new table
				CopyTable(current_table, index_set);
			}

		}



		/// <summary>
		/// Gets or sets if the conglomerate commit procedure should check for
		/// dirty selects and produce a transaction error.
		/// </summary>
		/// <remarks>
		/// A dirty select is when a query reads information from a table 
		/// that is effected by another table during a transaction. This in 
		/// itself will not cause data consistancy problems but for strict 
		/// conformance to <see cref="System.Data.IsolationLevel.Serializable"/>
		/// isolation level this should return true.
		/// <para>
		/// <b>Note</b> We <b>must not</b> make this method serialized because 
		/// it is back called from within a commit lock in TableDataConglomerate.
		/// </remarks>
		/// <returns></returns>
		internal bool TransactionErrorOnDirtySelect {
			get { return transaction_error_on_dirty_select; }
			set { transaction_error_on_dirty_select = value; }
		}


		// ----- Setting/Querying constraint information -----
		// PENDING: Is it worth implementing a pluggable constraint architecture
		//   as described in the idea below.  With the current implementation we
		//   have tied a DataTableConglomerate to a specific constraint
		//   architecture.
		//
		// IDEA: These methods delegate to the parent conglomerate which has a
		//   pluggable architecture for setting/querying constraints.  Some uses of
		//   a conglomerate may not need integrity constraints or may implement the
		//   mechanism for storing/querying in a different way.  This provides a
		//   useful abstraction of being enable to implement constraint behaviour
		//   by only providing a way to set/query the constraint information in
		//   different conglomerate uses.

		/// <summary>
		/// Convenience, given a <see cref="SimpleTableQuery"/> object this 
		/// will return a list of column names in sequence that represent the 
		/// columns in a group constraint.
		/// </summary>
		/// <param name="dt"></param>
		/// <param name="cols">The unsorted list of indexes in the table that 
		/// represent the group.</param>
		/// <remarks>
		/// Assumes column 2 of dt is the sequence number and column 1 is the name
		/// of the column.
		/// </remarks>
		/// <returns></returns>
		private static String[] ToColumns(SimpleTableQuery dt, IntegerVector cols) {
			int size = cols.Count;
			String[] list = new String[size];

			// for each n of the output list
			for (int n = 0; n < size; ++n) {
				// for each i of the input list
				for (int i = 0; i < size; ++i) {
					int row_index = cols[i];
					int seq_no = ((BigNumber)dt.Get(2, row_index).Object).ToInt32();
					if (seq_no == n) {
						list[n] = dt.Get(1, row_index).Object.ToString();
						break;
					}
				}
			}

			return list;
		}

		/// <summary>
		/// Generates a unique constraint name.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="unique_id"></param>
		/// <remarks>
		/// If the given constraint name is 'null' then a new one is created, 
		/// otherwise the given default one is returned.
		/// </remarks>
		/// <returns></returns>
		private static String MakeUniqueConstraintName(String name,
													   BigNumber unique_id) {
			if (name == null) {
				name = "_ANONYMOUS_CONSTRAINT_" + unique_id.ToString();
			}
			return name;
		}

		/// <summary>
		/// Notifies this transaction that a database object with the given 
		/// name has successfully been created.
		/// </summary>
		/// <param name="table_name"></param>
		internal void OnDatabaseObjectCreated(TableName table_name) {
			// If this table name was dropped, then remove from the drop list
			bool dropped = dropped_database_objects.Contains(table_name);
			dropped_database_objects.Remove(table_name);
			// If the above operation didn't remove a table name then add to the
			// created database objects list.
			if (!dropped) {
				created_database_objects.Add(table_name);
			}
		}

		/// <summary>
		/// Notifies this transaction that a database object with the given 
		/// name has successfully been dropped.
		/// </summary>
		/// <param name="table_name"></param>
		internal void OnDatabaseObjectDropped(TableName table_name) {
			// If this table name was created, then remove from the create list
			bool created = created_database_objects.Contains(table_name);
			created_database_objects.Remove(table_name);
			// If the above operation didn't remove a table name then add to the
			// dropped database objects list.
			if (!created) {
				dropped_database_objects.Add(table_name);
			}
		}

		/// <summary>
		/// Returns the normalized list of database object names created 
		/// in this transaction.
		/// </summary>
		internal ArrayList AllNamesCreated {
			get { return created_database_objects; }
		}

		/// <summary>
		/// Returns the normalized list of database object names dropped 
		/// in this transaction.
		/// </summary>
		internal ArrayList AllNamesDropped {
			get { return dropped_database_objects; }
		}


		/// <summary>
		/// Create a new schema in this transaction.
		/// </summary>
		/// <param name="name">The name of the schema to create.</param>
		/// <param name="type">The type to assign to the schema.</param>
		/// <remarks>
		/// When the transaction is committed the schema will become globally 
		/// accessable.
		/// <para>
		/// Any security checks must be performed before this method is called.
		/// </para>
		/// <para>
		/// <b>Note</b>: We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		/// <exception cref="StatementException">
		/// If a schema with the same <paramref name="name"/> already exists.
		/// </exception>
		public void CreateSchema(String name, String type) {
			TableName table_name = TableDataConglomerate.SCHEMA_INFO_TABLE;
			IMutableTableDataSource t = GetTable(table_name);
			SimpleTableQuery dt = new SimpleTableQuery(t);

			try {
				// Select entries where;
				//     sUSRSchemaInfo.name = name
				if (!dt.Exists(1, name)) {
					// Add the entry to the schema info table.
					RowData rd = new RowData(t);
					BigNumber unique_id = NextUniqueID(table_name);
					rd.SetColumnDataFromObject(0, unique_id);
					rd.SetColumnDataFromObject(1, name);
					rd.SetColumnDataFromObject(2, type);
					// Third (other) column is left as null
					t.AddRow(rd);
				} else {
					throw new StatementException("Schema already exists: " + name);
				}
			} finally {
				dt.Dispose();
			}
		}

		/// <summary>
		/// Drops a new schema in this transaction.
		/// </summary>
		/// <param name="name">The name of the schema to drop.</param>
		/// <remarks>
		/// When the transaction is committed the schema will become globally 
		/// accessable.
		/// <para>
		/// Note that any security checks must be performed before this method 
		/// is called.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		public void DropSchema(String name) {
			TableName table_name = TableDataConglomerate.SCHEMA_INFO_TABLE;
			IMutableTableDataSource t = GetTable(table_name);
			SimpleTableQuery dt = new SimpleTableQuery(t);

			// Drop a single entry from dt where column 1 = name
			bool b = dt.Delete(1, name);
			dt.Dispose();
			if (!b) {
				throw new StatementException("Schema doesn't exists: " + name);
			}
		}

		/// <summary>
		/// Returns true if the schema exists within this transaction.
		/// </summary>
		/// <param name="name"></param>
		/// <returns></returns>
		public bool SchemaExists(String name) {
			TableName table_name = TableDataConglomerate.SCHEMA_INFO_TABLE;
			IMutableTableDataSource t = GetTable(table_name);
			SimpleTableQuery dt = new SimpleTableQuery(t);

			// Returns true if there's a single entry in dt where column 1 = name
			bool b = dt.Exists(1, name);
			dt.Dispose();
			return b;
		}

		/// <summary>
		/// Resolves the case of the given schema name if the database is 
		/// performing case insensitive identifier matching.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="ignore_case"></param>
		/// <returns>
		/// Returns a SchemaDef object that identifiers the schema. 
		/// Returns null if the schema name could not be resolved.
		/// </returns>
		public SchemaDef ResolveSchemaCase(String name, bool ignore_case) {
			// The list of schema
			SimpleTableQuery dt = new SimpleTableQuery(
								 GetTable(TableDataConglomerate.SCHEMA_INFO_TABLE));

			try {
				IRowEnumerator e = dt.GetRowEnumerator();
				if (ignore_case) {
					SchemaDef result = null;
					while (e.MoveNext()) {
						int row_index = e.RowIndex;
						String cur_name = dt.Get(1, row_index).Object.ToString();
						if (String.Compare(name, cur_name, true) == 0) {
							if (result != null) {
								throw new StatementException(
													   "Ambiguous schema name: '" + name + "'");
							}
							String type = dt.Get(2, row_index).Object.ToString();
							result = new SchemaDef(cur_name, type);
						}
					}
					return result;

				} else {  // if (!ignore_case)
					while (e.MoveNext()) {
						int row_index = e.RowIndex;
						String cur_name = dt.Get(1, row_index).Object.ToString();
						if (name.Equals(cur_name)) {
							String type = dt.Get(2, row_index).Object.ToString();
							return new SchemaDef(cur_name, type);
						}
					}
					// Not found
					return null;
				}
			} finally {
				dt.Dispose();
			}

		}

		/// <summary>
		/// Returns an array of <see cref="SchemaDef"/> objects for each schema 
		/// currently setup in the database.
		/// </summary>
		/// <returns></returns>
		public SchemaDef[] GetSchemaList() {
			// The list of schema
			SimpleTableQuery dt = new SimpleTableQuery(
								 GetTable(TableDataConglomerate.SCHEMA_INFO_TABLE));
			IRowEnumerator e = dt.GetRowEnumerator();
			SchemaDef[] arr = new SchemaDef[dt.RowCount];
			int i = 0;

			while (e.MoveNext()) {
				int row_index = e.RowIndex;
				String cur_name = dt.Get(1, row_index).Object.ToString();
				String cur_type = dt.Get(2, row_index).Object.ToString();
				arr[i] = new SchemaDef(cur_name, cur_type);
				++i;
			}

			dt.Dispose();
			return arr;
		}

		/// <summary>
		/// Sets a persistent variable of the database that becomes a committed
		/// change once this transaction is committed.
		/// </summary>
		/// <param name="variable"></param>
		/// <param name="value"></param>
		/// <remarks>
		/// The variable can later be retrieved with a call to the 
		/// <see cref="GetPersistantVariable"/> method.  A persistant var is created 
		/// if it doesn't exist in the DatabaseVars table otherwise it is 
		/// overwritten.
		/// </remarks>
		public void SetPersistentVariable(String variable, String value) {
			TableName table_name = TableDataConglomerate.PERSISTENT_VAR_TABLE;
			IMutableTableDataSource t = GetTable(table_name);
			SimpleTableQuery dt = new SimpleTableQuery(t);
			dt.SetVariable(0, new Object[] { variable, value });
			dt.Dispose();
		}

		/// <summary>
		/// Returns the value of the persistent variable with the given name 
		/// or null if it doesn't exist.
		/// </summary>
		/// <param name="variable"></param>
		/// <returns></returns>
		public String GetPersistantVariable(String variable) {
			TableName table_name = TableDataConglomerate.PERSISTENT_VAR_TABLE;
			IMutableTableDataSource t = GetTable(table_name);
			SimpleTableQuery dt = new SimpleTableQuery(t);
			String val = dt.GetVariable(1, 0, variable).ToString();
			dt.Dispose();
			return val;
		}

		/// <summary>
		/// Creates a new sequence generator with the given TableName and 
		/// initializes it with the given details.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="start_value"></param>
		/// <param name="increment_by"></param>
		/// <param name="min_value"></param>
		/// <param name="max_value"></param>
		/// <param name="cache"></param>
		/// <param name="cycle"></param>
		/// <remarks>
		/// This does <b>not</b> check if the given name clashes with an existing 
		/// database object.
		/// </remarks>
		public void CreateSequenceGenerator(
					 TableName name, long start_value, long increment_by,
					 long min_value, long max_value, long cache, bool cycle) {
			SequenceManager.CreateSequenceGenerator(this,
				 name, start_value, increment_by, min_value, max_value, cache,
				 cycle);

			// Notify that this database object has been created
			OnDatabaseObjectCreated(name);
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

		/// <summary>
		/// Adds a unique constraint to the database which becomes perminant 
		/// when the transaction is committed.
		/// </summary>
		/// <param name="table_name"></param>
		/// <param name="cols"></param>
		/// <param name="deferred"></param>
		/// <param name="constraint_name"></param>
		/// <remarks>
		/// Columns in a table that are defined as unique are prevented from 
		/// being duplicated by the engine.
		/// <para>
		/// <b>Note</b> Security checks for adding constraints must be checked 
		/// for at a higher layer.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		public void AddUniqueConstraint(TableName table_name,
						  String[] cols, ConstraintDeferrability deferred, String constraint_name) {

			TableName tn1 = TableDataConglomerate.UNIQUE_INFO_TABLE;
			TableName tn2 = TableDataConglomerate.UNIQUE_COLS_TABLE;
			IMutableTableDataSource t = GetTable(tn1);
			IMutableTableDataSource tcols = GetTable(tn2);

			try {

				// Insert a value into UNIQUE_INFO_TABLE
				RowData rd = new RowData(t);
				BigNumber unique_id = NextUniqueID(tn1);
				constraint_name = MakeUniqueConstraintName(constraint_name, unique_id);
				rd.SetColumnDataFromObject(0, unique_id);
				rd.SetColumnDataFromObject(1, constraint_name);
				rd.SetColumnDataFromObject(2, table_name.Schema);
				rd.SetColumnDataFromObject(3, table_name.Name);
				rd.SetColumnDataFromObject(4, (BigNumber)((short)deferred));
				t.AddRow(rd);

				// Insert the columns
				for (int i = 0; i < cols.Length; ++i) {
					rd = new RowData(tcols);
					rd.SetColumnDataFromObject(0, unique_id);            // unique id
					rd.SetColumnDataFromObject(1, cols[i]);              // column name
					rd.SetColumnDataFromObject(2, (BigNumber)i);         // sequence number
					tcols.AddRow(rd);
				}

			} catch (DatabaseConstraintViolationException e) {
				// Constraint violation when inserting the data.  Check the type and
				// wrap around an appropriate error message.
				if (e.ErrorCode ==
						  DatabaseConstraintViolationException.UniqueViolation) {
					// This means we gave a constraint name that's already being used
					// for a primary key.
					throw new StatementException(
									"Unique constraint name '" + constraint_name +
									"' is already being used.");
				}
				throw e;
			}

		}

		/// <summary>
		/// Adds a foreign key constraint to the database which becomes perminent
		/// when the transaction is committed.
		/// </summary>
		/// <param name="table">The key table to link from.</param>
		/// <param name="cols">The key columns to link from</param>
		/// <param name="ref_table">The referenced table to link to.</param>
		/// <param name="ref_cols">The refenced columns to link to.</param>
		/// <param name="delete_rule">The rule called during cascade delete.</param>
		/// <param name="update_rule">The rule called during cascade update.</param>
		/// <param name="deferred"></param>
		/// <param name="constraint_name">The name of the constraint to create.</param>
		/// <remarks>
		/// A foreign key represents a referential link from one table to 
		/// another (may be the same table).
		/// <para>
		/// <b>Note</b> Security checks for adding constraints must be checked 
		/// for at a higher layer.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		public void AddForeignKeyConstraint(TableName table, String[] cols,
											TableName ref_table, String[] ref_cols,
											ConstraintAction delete_rule, ConstraintAction update_rule,
											ConstraintDeferrability deferred, String constraint_name) {
			TableName tn1 = TableDataConglomerate.FOREIGN_INFO_TABLE;
			TableName tn2 = TableDataConglomerate.FOREIGN_COLS_TABLE;
			IMutableTableDataSource t = GetTable(tn1);
			IMutableTableDataSource tcols = GetTable(tn2);

			try {

				// If 'ref_columns' empty then set to primary key for referenced table,
				// ISSUE: What if primary key changes after the fact?
				if (ref_cols.Length == 0) {
					ColumnGroup set = QueryTablePrimaryKeyGroup(this, ref_table);
					if (set == null) {
						throw new StatementException(
						  "No primary key defined for referenced table '" + ref_table + "'");
					}
					ref_cols = set.columns;
				}

				if (cols.Length != ref_cols.Length) {
					throw new StatementException("Foreign key reference '" + table +
					  "' -> '" + ref_table + "' does not have an equal number of " +
					  "column terms.");
				}

				// If delete or update rule is 'SET NULL' then check the foreign key
				// columns are not constrained as 'NOT NULL'
				if (delete_rule == ConstraintAction.SET_NULL ||
					update_rule == ConstraintAction.SET_NULL) {
					DataTableDef table_def = GetDataTableDef(table);
					for (int i = 0; i < cols.Length; ++i) {
						DataTableColumnDef column_def =
									  table_def[table_def.FindColumnName(cols[i])];
						if (column_def.IsNotNull) {
							throw new StatementException("Foreign key reference '" + table +
								   "' -> '" + ref_table + "' update or delete triggered " +
								   "action is SET NULL for columns that are constrained as " +
								   "NOT NULL.");
						}
					}
				}

				// Insert a value into FOREIGN_INFO_TABLE
				RowData rd = new RowData(t);
				BigNumber unique_id = NextUniqueID(tn1);
				constraint_name = MakeUniqueConstraintName(constraint_name, unique_id);
				rd.SetColumnDataFromObject(0, unique_id);
				rd.SetColumnDataFromObject(1, constraint_name);
				rd.SetColumnDataFromObject(2, table.Schema);
				rd.SetColumnDataFromObject(3, table.Name);
				rd.SetColumnDataFromObject(4, ref_table.Schema);
				rd.SetColumnDataFromObject(5, ref_table.Name);
				rd.SetColumnDataFromObject(6, (BigNumber)((int)update_rule));
				rd.SetColumnDataFromObject(7, (BigNumber)((int)delete_rule));
				rd.SetColumnDataFromObject(8, (BigNumber)((short)deferred));
				t.AddRow(rd);

				// Insert the columns
				for (int i = 0; i < cols.Length; ++i) {
					rd = new RowData(tcols);
					rd.SetColumnDataFromObject(0, unique_id);            // unique id
					rd.SetColumnDataFromObject(1, cols[i]);              // column name
					rd.SetColumnDataFromObject(2, ref_cols[i]);          // ref column name
					rd.SetColumnDataFromObject(3, (BigNumber)i); // sequence number
					tcols.AddRow(rd);
				}

			} catch (DatabaseConstraintViolationException e) {
				// Constraint violation when inserting the data.  Check the type and
				// wrap around an appropriate error message.
				if (e.ErrorCode ==
						  DatabaseConstraintViolationException.UniqueViolation) {
					// This means we gave a constraint name that's already being used
					// for a primary key.
					throw new StatementException("Foreign key constraint name '" +
										   constraint_name + "' is already being used.");
				}
				throw e;
			}

		}

		/// <summary>
		/// Adds a primary key constraint that becomes perminent when the 
		/// transaction is committed.
		/// </summary>
		/// <param name="table_name"></param>
		/// <param name="cols"></param>
		/// <param name="deferred"></param>
		/// <param name="constraint_name"></param>
		/// <remarks>
		/// A primary key represents a set of columns in a table that are 
		/// constrained to be unique and can not be null. If the constraint 
		/// name parameter is 'null' a primary key constraint is created with 
		/// a unique constraint name.
		/// <para>
		/// <b>Note</b> Security checks for adding constraints must be checked 
		/// for at a higher layer.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		public void AddPrimaryKeyConstraint(TableName table_name, String[] cols,
											ConstraintDeferrability deferred, String constraint_name) {

			TableName tn1 = TableDataConglomerate.PRIMARY_INFO_TABLE;
			TableName tn2 = TableDataConglomerate.PRIMARY_COLS_TABLE;
			IMutableTableDataSource t = GetTable(tn1);
			IMutableTableDataSource tcols = GetTable(tn2);

			try {

				// Insert a value into PRIMARY_INFO_TABLE
				RowData rd = new RowData(t);
				BigNumber unique_id = NextUniqueID(tn1);
				constraint_name = MakeUniqueConstraintName(constraint_name, unique_id);
				rd.SetColumnDataFromObject(0, unique_id);
				rd.SetColumnDataFromObject(1, constraint_name);
				rd.SetColumnDataFromObject(2, table_name.Schema);
				rd.SetColumnDataFromObject(3, table_name.Name);
				rd.SetColumnDataFromObject(4, (BigNumber)((short)deferred));
				t.AddRow(rd);

				// Insert the columns
				for (int i = 0; i < cols.Length; ++i) {
					rd = new RowData(tcols);
					rd.SetColumnDataFromObject(0, unique_id);            // unique id
					rd.SetColumnDataFromObject(1, cols[i]);              // column name
					rd.SetColumnDataFromObject(2, (BigNumber)i);         // Sequence number
					tcols.AddRow(rd);
				}

			} catch (DatabaseConstraintViolationException e) {
				// Constraint violation when inserting the data.  Check the type and
				// wrap around an appropriate error message.
				if (e.ErrorCode ==
						  DatabaseConstraintViolationException.UniqueViolation) {
					// This means we gave a constraint name that's already being used
					// for a primary key.
					throw new StatementException("Primary key constraint name '" +
										   constraint_name + "' is already being used.");
				}
				throw e;
			}

		}

		/// <summary>
		/// Adds a check expression that becomes perminent when the transaction
		/// is committed.
		/// </summary>
		/// <param name="table_name"></param>
		/// <param name="expression"></param>
		/// <param name="deferred"></param>
		/// <param name="constraint_name"></param>
		/// <remarks>
		/// A check expression is an expression that must evaluate to true 
		/// for all records added/updated in the database.
		/// <para>
		/// <b>Note</b> Security checks for adding constraints must be checked 
		/// for at a higher layer.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		public void AddCheckConstraint(TableName table_name,
					 Expression expression, ConstraintDeferrability deferred, String constraint_name) {

			TableName tn = TableDataConglomerate.CHECK_INFO_TABLE;
			IMutableTableDataSource t = GetTable(tn);
			int col_count = t.DataTableDef.ColumnCount;

			try {

				// Insert check constraint data.
				BigNumber unique_id = NextUniqueID(tn);
				constraint_name = MakeUniqueConstraintName(constraint_name, unique_id);
				RowData rd = new RowData(t);
				rd.SetColumnDataFromObject(0, unique_id);
				rd.SetColumnDataFromObject(1, constraint_name);
				rd.SetColumnDataFromObject(2, table_name.Schema);
				rd.SetColumnDataFromObject(3, table_name.Name);
				rd.SetColumnDataFromObject(4, expression.Text.ToString());
				rd.SetColumnDataFromObject(5, (BigNumber)((short)deferred));
				if (col_count > 6) {
					// Serialize the check expression
					ByteLongObject serialized_expression =
											  ObjectTranslator.Serialize(expression);
					rd.SetColumnDataFromObject(6, serialized_expression);
				}
				t.AddRow(rd);

			} catch (DatabaseConstraintViolationException e) {
				// Constraint violation when inserting the data.  Check the type and
				// wrap around an appropriate error message.
				if (e.ErrorCode ==
						  DatabaseConstraintViolationException.UniqueViolation) {
					// This means we gave a constraint name that's already being used.
					throw new StatementException("Check constraint name '" +
										   constraint_name + "' is already being used.");
				}
				throw e;
			}

		}

		/// <summary>
		/// Drops all the constraints defined for the given table.
		/// </summary>
		/// <param name="table_name"></param>
		/// <remarks>
		/// This is a useful function when dropping a table from the database.
		/// <para>
		/// <b>Note</b> Security checks for dropping constraints must be checked 
		/// for at a higher layer.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		public void DropAllConstraintsForTable(TableName table_name) {
			ColumnGroup primary = QueryTablePrimaryKeyGroup(this, table_name);
			ColumnGroup[] uniques = QueryTableUniqueGroups(this, table_name);
			CheckExpression[] expressions =
										 QueryTableCheckExpressions(this, table_name);
			ColumnGroupReference[] refs =
									 QueryTableForeignKeyReferences(this, table_name);

			if (primary != null) {
				DropPrimaryKeyConstraintForTable(table_name, primary.name);
			}
			for (int i = 0; i < uniques.Length; ++i) {
				DropUniqueConstraintForTable(table_name, uniques[i].name);
			}
			for (int i = 0; i < expressions.Length; ++i) {
				DropCheckConstraintForTable(table_name, expressions[i].name);
			}
			for (int i = 0; i < refs.Length; ++i) {
				DropForeignKeyReferenceConstraintForTable(table_name, refs[i].name);
			}

		}

		/// <summary>
		/// Drops the named constraint from the transaction.
		/// </summary>
		/// <param name="table_name"></param>
		/// <param name="constraint_name"></param>
		/// <remarks>
		/// Used when altering table schema. Returns the number of constraints 
		/// that were removed from the system. If this method returns 0 then 
		/// it indicates there is no constraint with the given name in the 
		/// table.
		/// <para>
		/// <b>Note</b> Security checks for dropping constraints must be checked 
		/// for at a higher layer.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns the actual count of dropped constraints.
		/// </returns>
		public int DropNamedConstraint(TableName table_name,
									   String constraint_name) {

			int drop_count = 0;
			if (DropPrimaryKeyConstraintForTable(table_name, constraint_name)) {
				++drop_count;
			}
			if (DropUniqueConstraintForTable(table_name, constraint_name)) {
				++drop_count;
			}
			if (DropCheckConstraintForTable(table_name, constraint_name)) {
				++drop_count;
			}
			if (DropForeignKeyReferenceConstraintForTable(table_name,
														  constraint_name)) {
				++drop_count;
			}
			return drop_count;
		}

		/// <summary>
		/// Drops the primary key constraint for the given table.
		/// </summary>
		/// <param name="table_name"></param>
		/// <param name="constraint_name"></param>
		/// <remarks>
		/// Used when altering table schema. If 'constraint_name' is null this 
		/// method will search for the primary key of the table name. 
		/// <para>
		/// <b>Note</b> Security checks for dropping constraints must be checked 
		/// for at a higher layer.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns true if the primary key constraint was dropped (the 
		/// constraint existed), otherwise false.
		/// </returns>
		public bool DropPrimaryKeyConstraintForTable(TableName table_name, String constraint_name) {
			IMutableTableDataSource t =
								 GetTable(TableDataConglomerate.PRIMARY_INFO_TABLE);
			IMutableTableDataSource t2 =
								 GetTable(TableDataConglomerate.PRIMARY_COLS_TABLE);
			SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
			SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

			try {
				IntegerVector data;
				if (constraint_name != null) {
					// Returns the list of indexes where column 1 = constraint name
					//                               and column 2 = schema name
					data = dt.SelectEqual(1, constraint_name,
												 2, table_name.Schema);
				} else {
					// Returns the list of indexes where column 3 = table name
					//                               and column 2 = schema name
					data = dt.SelectEqual(3, table_name.Name,
												 2, table_name.Schema);
				}

				if (data.Count > 1) {
					throw new ApplicationException("Assertion failed: multiple primary key for: " +
									table_name);
				} else if (data.Count == 1) {
					int row_index = data[0];
					// The id
					TObject id = dt.Get(0, row_index);
					// All columns with this id
					IntegerVector ivec = dtcols.SelectEqual(0, id);
					// Delete from the table
					dtcols.DeleteRows(ivec);
					dt.DeleteRows(data);
					return true;
				}
				// data.size() must be 0 so no constraint was found to drop.
				return false;

			} finally {
				dtcols.Dispose();
				dt.Dispose();
			}

		}

		/// <summary>
		/// Drops a single named unique constraint from the given table.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="constraint_name"></param>
		/// <remarks>
		/// <para>
		/// <b>Note</b> Security checks for dropping constraints must be checked 
		/// for at a higher layer.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns true if the unique constraint was dropped (the constraint 
		/// existed), otherwise false.
		/// </returns>
		public bool DropUniqueConstraintForTable(
										 TableName table, String constraint_name) {

			IMutableTableDataSource t =
								 GetTable(TableDataConglomerate.UNIQUE_INFO_TABLE);
			IMutableTableDataSource t2 =
								 GetTable(TableDataConglomerate.UNIQUE_COLS_TABLE);
			SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
			SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

			try {
				// Returns the list of indexes where column 1 = constraint name
				//                               and column 2 = schema name
				IntegerVector data = dt.SelectEqual(1, constraint_name,
														   2, table.Schema);

				if (data.Count > 1) {
					throw new ApplicationException("Assertion failed: multiple unique constraint name: " +
									constraint_name);
				} else if (data.Count == 1) {
					int row_index = data[0];
					// The id
					TObject id = dt.Get(0, row_index);
					// All columns with this id
					IntegerVector ivec = dtcols.SelectEqual(0, id);
					// Delete from the table
					dtcols.DeleteRows(ivec);
					dt.DeleteRows(data);
					return true;
				}
				// data.size() == 0 so the constraint wasn't found
				return false;
			} finally {
				dtcols.Dispose();
				dt.Dispose();
			}

		}

		/// <summary>
		/// Drops a single named check constraint from the given table.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="constraint_name"></param>
		/// <remarks>
		/// <para>
		/// <b>Note</b> Security checks for dropping constraints must be checked 
		/// for at a higher layer.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns true if the check constraint was dropped (the constraint 
		/// existed), otherwise false.
		/// </returns>
		public bool DropCheckConstraintForTable(
										 TableName table, String constraint_name) {

			IMutableTableDataSource t =
								 GetTable(TableDataConglomerate.CHECK_INFO_TABLE);
			SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table

			try {
				// Returns the list of indexes where column 1 = constraint name
				//                               and column 2 = schema name
				IntegerVector data = dt.SelectEqual(1, constraint_name,
														   2, table.Schema);

				if (data.Count > 1) {
					throw new ApplicationException("Assertion failed: multiple check constraint name: " +
									constraint_name);
				} else if (data.Count == 1) {
					// Delete the check constraint
					dt.DeleteRows(data);
					return true;
				}
				// data.size() == 0 so the constraint wasn't found
				return false;
			} finally {
				dt.Dispose();
			}

		}

		/// <summary>
		/// Drops a single named foreign key reference from the given table.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="constraint_name"></param>
		/// <remarks>
		/// <para>
		/// <b>Note</b> Security checks for dropping constraints must be checked 
		/// for at a higher layer.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns true if the foreign key reference constraint was dropped 
		/// (the constraint existed), otherwise false.
		/// </returns>
		public bool DropForeignKeyReferenceConstraintForTable(
										 TableName table, String constraint_name) {

			IMutableTableDataSource t =
								 GetTable(TableDataConglomerate.FOREIGN_INFO_TABLE);
			IMutableTableDataSource t2 =
								 GetTable(TableDataConglomerate.FOREIGN_COLS_TABLE);
			SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
			SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

			try {
				// Returns the list of indexes where column 1 = constraint name
				//                               and column 2 = schema name
				IntegerVector data = dt.SelectEqual(1, constraint_name,
														   2, table.Schema);

				if (data.Count > 1) {
					throw new ApplicationException("Assertion failed: multiple foreign key constraint " +
									"name: " + constraint_name);
				} else if (data.Count == 1) {
					int row_index = data[0];
					// The id
					TObject id = dt.Get(0, row_index);
					// All columns with this id
					IntegerVector ivec = dtcols.SelectEqual(0, id);
					// Delete from the table
					dtcols.DeleteRows(ivec);
					dt.DeleteRows(data);
					return true;
				}
				// data.size() == 0 so the constraint wasn't found
				return false;
			} finally {
				dtcols.Dispose();
				dt.Dispose();
			}

		}

		/// <summary>
		/// Returns the list of tables (as a TableName array) that are dependant
		/// on the data in the given table to maintain referential consistancy.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="table"></param>
		/// <remarks>
		/// The list includes the tables referenced as foreign keys, and the 
		/// tables that reference the table as a foreign key.
		/// <para>
		/// This is a useful query for determining ahead of time the tables 
		/// that require a read lock when inserting/updating a table. A table
		/// will require a read lock if the operation needs to query it for 
		/// potential referential integrity violations.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public static TableName[] QueryTablesRelationallyLinkedTo(
							 SimpleTransaction transaction, TableName table) {
			ArrayList list = new ArrayList();
			ColumnGroupReference[] refs =
									QueryTableForeignKeyReferences(transaction, table);
			for (int i = 0; i < refs.Length; ++i) {
				TableName tname = refs[i].ref_table_name;
				if (!list.Contains(tname)) {
					list.Add(tname);
				}
			}
			refs = QueryTableImportedForeignKeyReferences(transaction, table);
			for (int i = 0; i < refs.Length; ++i) {
				TableName tname = refs[i].key_table_name;
				if (!list.Contains(tname)) {
					list.Add(tname);
				}
			}
			return (TableName[])list.ToArray(typeof(TableName));
		}

		/// <summary>
		/// Returns a set of unique groups that are constrained to be unique 
		/// for the given table in this transaction.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="table_name"></param>
		/// <remarks>
		/// For example, if columns ('name') and ('number', 'document_rev') 
		/// are defined as unique, this will return an array of two groups 
		/// that represent unique columns in the given table.
		/// </remarks>
		/// <returns></returns>
		public static ColumnGroup[] QueryTableUniqueGroups(
						SimpleTransaction transaction, TableName table_name) {
			ITableDataSource t =
			  transaction.GetTableDataSource(TableDataConglomerate.UNIQUE_INFO_TABLE);
			ITableDataSource t2 =
			  transaction.GetTableDataSource(TableDataConglomerate.UNIQUE_COLS_TABLE);
			SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
			SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

			ColumnGroup[] groups;
			try {
				// Returns the list indexes where column 3 = table name
				//                            and column 2 = schema name
				IntegerVector data = dt.SelectEqual(3, table_name.Name,
														   2, table_name.Schema);
				groups = new ColumnGroup[data.Count];

				for (int i = 0; i < data.Count; ++i) {
					TObject id = dt.Get(0, data[i]);

					// Select all records with equal id
					IntegerVector cols = dtcols.SelectEqual(0, id);

					// Put into a group.
					ColumnGroup group = new ColumnGroup();
					// constraint name
					group.name = dt.Get(1, data[i]).Object.ToString();
					group.columns = ToColumns(dtcols, cols);   // the list of columns
					group.deferred = (ConstraintDeferrability) ((BigNumber)dt.Get(4, data[i]).Object).ToInt16();
					groups[i] = group;
				}
			} finally {
				dt.Dispose();
				dtcols.Dispose();
			}

			return groups;
		}

		/// <summary>
		/// Returns a set of primary key groups that are constrained to be unique
		/// for the given table in this transaction (there can be only 1 primary
		/// key defined for a table).
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="table_name"></param>
		/// <returns>
		/// Returns null if there is no primary key defined for the table.
		/// </returns>
		public static ColumnGroup QueryTablePrimaryKeyGroup(
						SimpleTransaction transaction, TableName table_name) {
			ITableDataSource t =
			  transaction.GetTableDataSource(TableDataConglomerate.PRIMARY_INFO_TABLE);
			ITableDataSource t2 =
			  transaction.GetTableDataSource(TableDataConglomerate.PRIMARY_COLS_TABLE);
			SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
			SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

			try {
				// Returns the list indexes where column 3 = table name
				//                            and column 2 = schema name
				IntegerVector data = dt.SelectEqual(3, table_name.Name,
														   2, table_name.Schema);

				if (data.Count > 1) {
					throw new ApplicationException("Assertion failed: multiple primary key for: " +
									table_name);
				} else if (data.Count == 1) {
					int row_index = data[0];
					// The id
					TObject id = dt.Get(0, row_index);
					// All columns with this id
					IntegerVector ivec = dtcols.SelectEqual(0, id);
					// Make it in to a columns object
					ColumnGroup group = new ColumnGroup();
					group.name = dt.Get(1, row_index).Object.ToString();
					group.columns = ToColumns(dtcols, ivec);
					group.deferred = (ConstraintDeferrability) ((BigNumber)dt.Get(4, row_index).Object).ToInt16();
					return group;
				} else {
					return null;
				}
			} finally {
				dt.Dispose();
				dtcols.Dispose();
			}

		}

		/// <summary>
		/// Returns a set of check expressions that are constrained over all 
		/// new columns added to the given table in this transaction.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="table_name"></param>
		/// <remarks>
		/// For example, we may want a column called 'serial_number' to be 
		/// constrained as CHECK serial_number LIKE '___-________-___'.
		/// </remarks>
		/// <returns></returns>
		public static CheckExpression[] QueryTableCheckExpressions(
						SimpleTransaction transaction, TableName table_name) {
			ITableDataSource t =
				transaction.GetTableDataSource(TableDataConglomerate.CHECK_INFO_TABLE);
			SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table

			CheckExpression[] checks;
			try {
				// Returns the list indexes where column 3 = table name
				//                            and column 2 = schema name
				IntegerVector data = dt.SelectEqual(3, table_name.Name,
														   2, table_name.Schema);
				checks = new CheckExpression[data.Count];

				for (int i = 0; i < checks.Length; ++i) {
					int row_index = data[i];

					CheckExpression check = new CheckExpression();
					check.name = dt.Get(1, row_index).Object.ToString();
					check.deferred = (ConstraintDeferrability) ((BigNumber)dt.Get(5, row_index).Object).ToInt16();
					// Is the deserialized version available?
					if (t.DataTableDef.ColumnCount > 6) {
						ByteLongObject sexp =
										   (ByteLongObject)dt.Get(6, row_index).Object;
						if (sexp != null) {
							try {
								// Deserialize the expression
								check.expression =
											  (Expression)ObjectTranslator.Deserialize(sexp);
							} catch (Exception e) {
								// We weren't able to deserialize the expression so report the
								// error to the log
								transaction.Debug.Write(DebugLevel.Warning, typeof (Transaction),
								            "Unable to deserialize the check expression.  " +
								            "The error is: " + e.Message);
								transaction.Debug.Write(DebugLevel.Warning, typeof (Transaction),
								            "Parsing the check expression instead.");

								check.expression = null;
							}
						}
					}
					// Otherwise we need to parse it from the string
					if (check.expression == null) {
						Expression exp = Expression.Parse(
											   dt.Get(4, row_index).Object.ToString());
						check.expression = exp;
					}
					checks[i] = check;
				}

			} finally {
				dt.Dispose();
			}

			return checks;
		}

		/// <summary>
		/// Returns an array of column references in the given table that 
		/// represent foreign key references.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="table_name"></param>
		/// <remarks>
		/// This method is used to check that a foreign key reference actually 
		/// points to a valid record in the referenced table as expected.
		/// </remarks>
		/// <returns></returns>
		/// <example>
		/// For example, say a foreign reference has been set up in the given 
		/// table as follows:
		/// <code>
		/// FOREIGN KEY (customer_id) REFERENCES Customer (id)
		/// </code>
		/// This method will return the column group reference
		/// Order(customer_id) -> Customer(id).
		/// </example>
		public static ColumnGroupReference[] QueryTableForeignKeyReferences(
						SimpleTransaction transaction, TableName table_name) {

			ITableDataSource t =
			  transaction.GetTableDataSource(TableDataConglomerate.FOREIGN_INFO_TABLE);
			ITableDataSource t2 =
			  transaction.GetTableDataSource(TableDataConglomerate.FOREIGN_COLS_TABLE);
			SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
			SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

			ColumnGroupReference[] groups;
			try {
				// Returns the list indexes where column 3 = table name
				//                            and column 2 = schema name
				IntegerVector data = dt.SelectEqual(3, table_name.Name,
														   2, table_name.Schema);
				groups = new ColumnGroupReference[data.Count];

				for (int i = 0; i < data.Count; ++i) {
					int row_index = data[i];

					// The foreign key id
					TObject id = dt.Get(0, row_index);

					// The referenced table
					TableName ref_table_name = new TableName(
							   dt.Get(4, row_index).Object.ToString(),
							   dt.Get(5, row_index).Object.ToString());

					// Select all records with equal id
					IntegerVector cols = dtcols.SelectEqual(0, id);

					// Put into a group.
					ColumnGroupReference group = new ColumnGroupReference();
					// constraint name
					group.name = dt.Get(1, row_index).Object.ToString();
					group.key_table_name = table_name;
					group.ref_table_name = ref_table_name;
					group.update_rule = (ConstraintAction) dt.Get(6, row_index).ToBigNumber().ToInt32();
					group.delete_rule = (ConstraintAction) dt.Get(7, row_index).ToBigNumber().ToInt32();
					group.deferred = (ConstraintDeferrability) ((BigNumber)dt.Get(8, row_index).Object).ToInt16();

					int cols_size = cols.Count;
					String[] key_cols = new String[cols_size];
					String[] ref_cols = new String[cols_size];
					for (int n = 0; n < cols_size; ++n) {
						for (int p = 0; p < cols_size; ++p) {
							int cols_index = cols[p];
							if (((BigNumber)dtcols.Get(3,
							                           cols_index).Object).ToInt32() == n) {
								key_cols[n] = dtcols.Get(1, cols_index).Object.ToString();
								ref_cols[n] = dtcols.Get(2, cols_index).Object.ToString();
								break;
							}
						}
					}
					group.key_columns = key_cols;
					group.ref_columns = ref_cols;

					groups[i] = group;
				}
			} finally {
				dt.Dispose();
				dtcols.Dispose();
			}

			return groups;
		}

		/// <summary>
		/// Returns an array of column references in the given table that represent
		/// foreign key references that reference columns in the given table.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="ref_table_name"></param>
		/// <remarks>
		/// This is a reverse mapping of the <see cref="QueryTableForeignKeyReferences"/>
		/// method.
		///	<para>
		///	This method is used to check that a reference isn't broken when we 
		///	remove a record (for example, removing a Customer that has references 
		///	to it will break integrity).
		///	</para>
		/// </remarks>
		/// <example>
		/// Say a foreign reference has been set up in any table as follows:
		/// <code>
		/// [ In table Order ]
		///		FOREIGN KEY (customer_id) REFERENCE Customer (id)
		/// </code>
		/// And the table name we are querying is <i>Customer</i> then this 
		/// method will return the column group reference
		/// <code>
		///		Order(customer_id) -> Customer(id).
		///	</code>
		/// </example>
		/// <returns></returns>
		public static ColumnGroupReference[] QueryTableImportedForeignKeyReferences(
					SimpleTransaction transaction, TableName ref_table_name) {

			ITableDataSource t =
			  transaction.GetTableDataSource(TableDataConglomerate.FOREIGN_INFO_TABLE);
			ITableDataSource t2 =
			  transaction.GetTableDataSource(TableDataConglomerate.FOREIGN_COLS_TABLE);
			SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
			SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

			ColumnGroupReference[] groups;
			try {
				// Returns the list indexes where column 5 = ref table name
				//                            and column 4 = ref schema name
				IntegerVector data = dt.SelectEqual(5, ref_table_name.Name,
														   4, ref_table_name.Schema);
				groups = new ColumnGroupReference[data.Count];

				for (int i = 0; i < data.Count; ++i) {
					int row_index = data[i];

					// The foreign key id
					TObject id = dt.Get(0, row_index);

					// The referencee table
					TableName table_name = new TableName(
						  dt.Get(2, row_index).Object.ToString(),
						  dt.Get(3, row_index).Object.ToString());

					// Select all records with equal id
					IntegerVector cols = dtcols.SelectEqual(0, id);

					// Put into a group.
					ColumnGroupReference group = new ColumnGroupReference();
					// constraint name
					group.name = dt.Get(1, row_index).Object.ToString();
					group.key_table_name = table_name;
					group.ref_table_name = ref_table_name;
					group.update_rule = (ConstraintAction) dt.Get(6, row_index).ToBigNumber().ToInt32();
					group.delete_rule = (ConstraintAction) dt.Get(7, row_index).ToBigNumber().ToInt32();
					group.deferred = (ConstraintDeferrability) ((BigNumber)dt.Get(8, row_index).Object).ToInt16();

					int cols_size = cols.Count;
					String[] key_cols = new String[cols_size];
					String[] ref_cols = new String[cols_size];
					for (int n = 0; n < cols_size; ++n) {
						for (int p = 0; p < cols_size; ++p) {
							int cols_index = cols[p];
							if (((BigNumber)dtcols.Get(3,
							                           cols_index).Object).ToInt32() == n) {
								key_cols[n] = dtcols.Get(1, cols_index).Object.ToString();
								ref_cols[n] = dtcols.Get(2, cols_index).Object.ToString();
								break;
							}
						}
					}
					group.key_columns = key_cols;
					group.ref_columns = ref_cols;

					groups[i] = group;
				}
			} finally {
				dt.Dispose();
				dtcols.Dispose();
			}

			return groups;
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
					conglomerate.ProcessCommit(this, VisibleTables,
											   selected_from_tables,
											   touched_tables, journal);
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
					conglomerate.ProcessRollback(this, touched_tables, journal);
				} finally {
					Cleanup();
				}
			}

		}

		/// <summary>
		/// Cleans up this transaction.
		/// </summary>
		private void Cleanup() {
			System.Stats.Decrement("Transaction.count");
			// Dispose of all the IIndexSet objects created by this transaction.
			DisposeAllIndices();

			// Dispose all the table we touched
			try {
				for (int i = 0; i < touched_tables.Count; ++i) {
					IMutableTableDataSource source =
										   (IMutableTableDataSource)touched_tables[i];
					source.Dispose();
				}
			} catch (Exception e) {
				Debug.WriteException(e);
			}

			System.Stats.Increment("Transaction.Cleanup");
			conglomerate = null;
			touched_tables = null;
			journal = null;

			// Dispose all the cursors in the transaction
			ClearCursors();

			Variables.Clear();
		}

		/**
		 * Disposes this transaction without rolling back or committing the changes.
		 * Care should be taken when using this - it must only be used for simple
		 * transactions that are short lived and have not modified the database.
		 */

		internal void dispose() {
			if (!IsReadOnly) {
				throw new Exception(
					"Assertion failed - tried to dispose a non Read-only transaction.");
			}
			if (!closed) {
				closed = true;
				Cleanup();
			}
		}

		// ---------- Cursor management ----------

		public Cursor DeclareCursor(TableName name, IQueryPlanNode queryPlan, CursorAttributes attributes) {
			if (cursors.ContainsKey(name))
				throw new ArgumentException("The cursor '" + name + "' was already defined within this transaction.");

			Cursor cursor = new Cursor(this, name, queryPlan, attributes);
			cursors[name] = cursor;

			OnDatabaseObjectCreated(name);

			return cursor;
		}

		public Cursor DeclareCursor(TableName name, IQueryPlanNode queryPlan) {
			return DeclareCursor(name, queryPlan, CursorAttributes.ReadOnly);
		}

		public Cursor GetCursor(TableName name) {
			if (name == null)
				throw new ArgumentNullException("name");

			Cursor cursor = cursors[name] as Cursor;
			if (cursor == null)
				throw new ArgumentException("Cursor '" + name + "' was not declared.");

			if (cursor.State == CursorState.Broken)
				throw new InvalidOperationException("The state of the cursor is invalid.");

			return cursor;
		}

		public void RemoveCursor(TableName name) {
			if (name == null)
				throw new ArgumentNullException("name");

			Cursor cursor = cursors[name] as Cursor;
			if (cursor == null)
				throw new ArgumentException("Cursor '" + name + "' was not declared.");

			cursor.InternalDispose();
			cursors.Remove(name);

			OnDatabaseObjectDropped(name);
		}

		public bool CursorExists(TableName name) {
			return cursors.ContainsKey(name);
		}

		protected void ClearCursors() {
			ArrayList cursorsList = new ArrayList(cursors.Values);
			for (int i = cursorsList.Count - 1; i >= 0; i--) {
				Cursor cursor = cursorsList[i] as Cursor;
				if (cursor == null)
					continue;

				cursor.Dispose();
			}

			cursors.Clear();
			cursors = null;
		}


		// ---------- Transaction inner classes ----------

		/// <summary>
		/// A list of DataTableDef system table definitions for tables internal 
		/// to the transaction.
		/// </summary>
		private readonly static DataTableDef[] INTERNAL_DEF_LIST;

		static Transaction() {
			INTERNAL_DEF_LIST = new DataTableDef[4];
			INTERNAL_DEF_LIST[0] = GTTableColumnsDataSource.DEF_DATA_TABLE_DEF;
			INTERNAL_DEF_LIST[1] = GTTableInfoDataSource.DEF_DATA_TABLE_DEF;
			INTERNAL_DEF_LIST[2] = GTProductDataSource.DEF_DATA_TABLE_DEF;
			INTERNAL_DEF_LIST[3] = GTVariablesDataSource.DEF_DATA_TABLE_DEF;
		}

		/// <summary>
		/// A static internal table info for internal tables to the transaction.
		/// </summary>
		/// <remarks>
		/// This implementation includes all the dynamically generated system tables
		/// that are tied to information in a transaction.
		/// </remarks>
		private class TransactionInternalTables : InternalTableInfo {
			private readonly Transaction transaction;

			public TransactionInternalTables(Transaction transaction)
				: base("SYSTEM TABLE", INTERNAL_DEF_LIST) {
				this.transaction = transaction;
			}

			// ---------- Implemented ----------

			public override IMutableTableDataSource CreateInternalTable(int index) {
				if (index == 0)
					return new GTTableColumnsDataSource(transaction).Init();
				if (index == 1)
					return new GTTableInfoDataSource(transaction).Init();
				if (index == 2)
					return new GTProductDataSource(transaction).Init();
				if (index == 3)
					return new GTVariablesDataSource(transaction).Init();
				
				throw new Exception();
			}

		}

		/// <summary>
		/// A group of columns in a table as used by the constraint system.
		/// </summary>
		public sealed class ColumnGroup {
			/// <summary>
			/// The name of the group (the constraint name).
			/// </summary>
			public String name;

			/// <summary>
			/// The list of columns that make up the group.
			/// </summary>
			public String[] columns;

			/// <summary>
			/// Whether this is deferred or initially immediate.
			/// </summary>
			public ConstraintDeferrability deferred;

		}

		/// <summary>
		/// Represents a constraint expression to check.
		/// </summary>
		public sealed class CheckExpression {
			/// <summary>
			/// The name of the check expression (the constraint name).
			/// </summary>
			public String name;

			/// <summary>
			/// The expression to check.
			/// </summary>
			public Expression expression;

			/// <summary>
			/// Whether this is deferred or initially immediate.
			/// </summary>
			public ConstraintDeferrability deferred;

		}

		/// <summary>
		/// Represents a reference from a group of columns in one table to a group of
		/// columns in another table.
		/// </summary>
		/// <remarks>
		/// This class is commonly used to represent a foreign key reference.
		/// </remarks>
		public sealed class ColumnGroupReference {

			///<summary>
			/// The name of the group (the constraint name).
			///</summary>
			public String name;

			///<summary>
			/// The key table name.
			///</summary>
			public TableName key_table_name;

			///<summary>
			/// The list of columns that make up the key.
			///</summary>
			public String[] key_columns;

			///<summary>
			/// The referenced table name.
			///</summary>
			public TableName ref_table_name;

			///<summary>
			/// The list of columns that make up the referenced group.
			///</summary>
			public String[] ref_columns;

			///<summary>
			/// The update rule.
			///</summary>
			public ConstraintAction update_rule;

			///<summary>
			/// The delete rule.
			///</summary>
			public ConstraintAction delete_rule;

			///<summary>
			/// Whether this is deferred or initially immediate.
			///</summary>
			public ConstraintDeferrability deferred;

		}

		#region Implementation of IDisposable

		void IDisposable.Dispose() {
			if (!closed) {
				Debug.Write(DebugLevel.Error, this, "Transaction not closed!");
				Rollback();
			}
		}

		#endregion
	}
}