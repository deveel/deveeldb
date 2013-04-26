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
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data {
	internal partial class Transaction {
		/// <summary>
		///  All tables touched by this transaction.  (IMutableTableDataSource)
		/// </summary>
		private List<IMutableTableDataSource> touchedTables;

		/// <summary>
		/// All tables selected from in this transaction.  (MasterTableDataSource)
		/// </summary>
		private readonly List<MasterTableDataSource> selectedFromTables;

		/// <summary>
		/// The list of IInternalTableInfo objects that are containers for generating
		/// internal tables (GTDataSource).
		/// </summary>
		private readonly IInternalTableInfo[] internalTables;

		/// <summary>
		/// A pointer in the internal_tables list.
		/// </summary>
		private int internalTablesIndex;

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
			if (internalTablesIndex >= internalTables.Length)
				throw new Exception("Internal table list bounds reached.");

			internalTables[internalTablesIndex] = info;
			++internalTablesIndex;
		}

		/// <summary>
		/// Called by the query evaluation layer when information is selected
		/// from this table as part of this transaction.
		/// </summary>
		/// <param name="tableName"></param>
		/// <remarks>
		/// When there is a select query on a table, when the transaction is 
		/// committed we should look for any concurrently committed changes 
		/// to the table.  If there are any, then any selects on the table 
		/// should be considered incorrect and cause a commit failure.
		/// </remarks>
		public void AddSelectedFromTable(TableName tableName) {
			// Special handling of internal tables,
			if (IsDynamicTable(tableName))
				return;

			MasterTableDataSource master = FindVisibleTable(tableName, false);
			if (master == null)
				throw new StatementException("Table with name not available: " + tableName);

			//    Console.Out.WriteLine("Selected from table: " + table_name);
			lock (selectedFromTables) {
				if (!selectedFromTables.Contains(master))
					selectedFromTables.Add(master);
			}
		}

		/// <inheritdoc/>
		protected override bool IsDynamicTable(TableName table_name) {
			foreach (IInternalTableInfo info in internalTables) {
				if (info != null && info.ContainsTableName(table_name))
					return true;
			}

			return false;
		}

		/// <inheritdoc/>
		protected override TableName[] GetDynamicTables() {
			int sz = 0;
			for (int i = 0; i < internalTables.Length; ++i) {
				IInternalTableInfo info = internalTables[i];
				if (info != null) {
					sz += info.TableCount;
				}
			}

			TableName[] list = new TableName[sz];
			int index = 0;

			for (int i = 0; i < internalTables.Length; ++i) {
				IInternalTableInfo info = internalTables[i];
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
		protected override DataTableInfo GetDynamicTableInfo(TableName tableName) {
			foreach (IInternalTableInfo info in internalTables) {
				if (info != null) {
					int index = info.FindTableName(tableName);
					if (index != -1)
						return info.GetTableInfo(index);
				}
			}

			throw new Exception("Not an internal table: " + tableName);
		}

		/// <inheritdoc/>
		protected override ITableDataSource GetDynamicTable(TableName tableName) {
			foreach (IInternalTableInfo info in internalTables) {
				if (info != null) {
					int index = info.FindTableName(tableName);
					if (index != -1)
						return info.CreateInternalTable(index);
				}
			}

			throw new Exception("Not an internal table: " + tableName);
		}

		/// <inheritdoc/>
		protected override string GetDynamicTableType(TableName tableName) {
			// Otherwise we need to look up the table in the internal table list,
			foreach (IInternalTableInfo info in internalTables) {
				if (info != null) {
					int index = info.FindTableName(tableName);
					if (index != -1)
						return info.GetTableType(index);
				}
			}
			// No internal table found, so report the error.
			throw new Exception("No table '" + tableName + "' to report type for.");
		}

		/// <summary>
		/// Creates a new table within this transaction with the given sector 
		/// size.
		/// </summary>
		/// <param name="tableInfo"></param>
		/// <param name="dataSectorSize"></param>
		/// <param name="indexSectorSize"></param>
		/// <remarks>
		/// This should only be called under an exclusive lock on the connection.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the table already exists.
		/// </exception>
		public void CreateTable(DataTableInfo tableInfo, int dataSectorSize, int indexSectorSize) {
			TableName tableName = tableInfo.TableName;
			MasterTableDataSource master = FindVisibleTable(tableName, false);
			if (master != null)
				throw new StatementException("Table '" + tableName + "' already exists.");

			tableInfo.IsReadOnly = true;

			if (dataSectorSize < 27) {
				dataSectorSize = 27;
			} else if (dataSectorSize > 4096) {
				dataSectorSize = 4096;
			}

			// Create the new master table and add to list of visible tables.
			master = conglomerate.CreateMasterTable(tableInfo, dataSectorSize, indexSectorSize);
			// Add this table (and an index set) for this table.
			AddVisibleTable(master, master.CreateIndexSet());

			// Log in the journal that this transaction touched the table_id.
			int tableId = master.TableId;
			journal.EntryAddTouchedTable(tableId);

			// Log in the journal that we created this table.
			journal.EntryTableCreate(tableId);

			// Add entry to the Sequences table for the native generator for this
			// table.
			SequenceManager.AddNativeTableGenerator(this, tableName);

			// Notify that this database object has been successfully created.
			OnDatabaseObjectCreated(tableName);
		}

		/// <summary>
		/// Creates a new table within this transaction with the given sector 
		/// size.
		/// </summary>
		/// <param name="tableInfo"></param>
		/// <remarks>
		/// This should only be called under an exclusive lock on the connection.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the table already exists.
		/// </exception>
		public void CreateTable(DataTableInfo tableInfo) {
			// data sector size defaults to 251
			// index sector size defaults to 1024
			CreateTable(tableInfo, 251, 1024);
		}

		public void CreateTemporaryTable(DataTableInfo tableInfo) {
			TableName tableName = tableInfo.TableName;
			MasterTableDataSource master = FindVisibleTable(tableName, false);
			if (master != null)
				throw new StatementException("Table '" + tableName + "' already exists.");

			tableInfo.IsReadOnly = true;

			MasterTableDataSource temp = conglomerate.CreateTemporaryDataSource(tableInfo);
			AddVisibleTable(temp, temp.CreateIndexSet());

			SequenceManager.AddNativeTableGenerator(this, tableName);

			// Notify that this database object has been successfully created.
			OnDatabaseObjectCreated(tableName);
		}

		/// <summary>
		/// Given a DataTableInfo, if the table exists then it is updated otherwise
		/// if it doesn't exist then it is created.
		/// </summary>
		/// <param name="tableInfo"></param>
		/// <param name="dataSectorSize"></param>
		/// <param name="indexSectorSize"></param>
		/// <remarks>
		/// This should only be used as very fine grain optimization for 
		/// creating/altering tables. If in the future the underlying table 
		/// model is changed so that the given <paramref name="dataSectorSize"/>
		/// and <paramref name="indexSectorSize"/> values are unapplicable, 
		/// then the value will be ignored.
		/// </remarks>
		public void AlterCreateTable(DataTableInfo tableInfo, int dataSectorSize, int indexSectorSize) {
			if (!TableExists(tableInfo.TableName)) {
				CreateTable(tableInfo, dataSectorSize, indexSectorSize);
			} else {
				AlterTable(tableInfo.TableName, tableInfo, dataSectorSize, indexSectorSize);
			}
		}

		/// <summary>
		/// Drops a table within this transaction.
		/// </summary>
		/// <param name="tableName"></param>
		/// <remarks>
		/// This should only be called under an exclusive lock on the connection.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the table does not exist.
		/// </exception>
		public void DropTable(TableName tableName) {
			MasterTableDataSource master = FindVisibleTable(tableName, false);

			if (master == null)
				throw new StatementException("Table '" + tableName + "' doesn't exist.");

			// Removes this table from the visible table list of this transaction
			RemoveVisibleTable(master);

			// Log in the journal that this transaction touched the table_id.
			int tableId = master.TableId;
			journal.EntryAddTouchedTable(tableId);

			// Log in the journal that we dropped this table.
			journal.EntryTableDrop(tableId);

			// Remove the native sequence generator (in this transaction) for this
			// table.
			SequenceManager.RemoveNativeTableGenerator(this, tableName);

			// Notify that this database object has been dropped
			OnDatabaseObjectDropped(tableName);
		}

		/// <summary>
		/// Generates an exact copy of the table within this transaction.
		/// </summary>
		/// <param name="srcMasterTable"></param>
		/// <param name="indexSet"></param>
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
		internal void CopyTable(MasterTableDataSource srcMasterTable, IIndexSet indexSet) {
			DataTableInfo tableInfo = srcMasterTable.TableInfo;
			TableName tableName = tableInfo.TableName;
			MasterTableDataSource master = FindVisibleTable(tableName, false);
			if (master != null)
				throw new StatementException("Unable to copy.  Table '" + tableName + "' already exists.");

			// Copy the master table and add to the list of visible tables.
			master = conglomerate.CopyMasterTable(srcMasterTable, indexSet);
			// Add this visible table
			AddVisibleTable(master, master.CreateIndexSet());

			// Log in the journal that this transaction touched the table_id.
			int tableId = master.TableId;
			journal.EntryAddTouchedTable(tableId);

			// Log in the journal that we created this table.
			journal.EntryTableCreate(tableId);

			// Add entry to the Sequences table for the native generator for this
			// table.
			SequenceManager.AddNativeTableGenerator(this, tableName);

			// Notify that this database object has been successfully created.
			OnDatabaseObjectCreated(tableName);
		}

		/// <summary>
		/// Alter the table with the given name to the new definition and give 
		/// the copied table a new data sector size.
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="tableInfo"></param>
		/// <param name="dataSectorSize"></param>
		/// <param name="indexSectorSize"></param>
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
		public void AlterTable(TableName tableName, DataTableInfo tableInfo, int dataSectorSize, int indexSectorSize) {
			tableInfo.IsReadOnly = true;

			// The current schema context is the schema of the table name
			string currentSchema = tableName.Schema;
			SystemQueryContext context = new SystemQueryContext(this, currentSchema);

			// Get the next unique id of the unaltered table.
			long nextId = NextUniqueID(tableName);

			// Drop the current table
			IMutableTableDataSource cTable = GetMutableTable(tableName);
			DropTable(tableName);

			// And create the table table
			CreateTable(tableInfo);
			IMutableTableDataSource alteredTable = GetMutableTable(tableName);

			// Get the new MasterTableDataSource object
			MasterTableDataSource newMasterTable = FindVisibleTable(tableName, false);
			// Set the sequence id of the table
			newMasterTable.SetUniqueID(nextId);

			// Work out which columns we have to copy to where
			int[] colMap = new int[tableInfo.ColumnCount];
			DataTableInfo origTd = cTable.TableInfo;
			for (int i = 0; i < colMap.Length; ++i) {
				string colName = tableInfo[i].Name;
				colMap[i] = origTd.FindColumnName(colName);
			}

			try {
				// First move all the rows from the old table to the new table,
				// This does NOT update the indexes.
				try {
					IRowEnumerator e = cTable.GetRowEnumerator();
					while (e.MoveNext()) {
						int rowIndex = e.RowIndex;
						DataRow dataRow = new DataRow(alteredTable);
						for (int i = 0; i < colMap.Length; ++i) {
							int col = colMap[i];
							if (col != -1) {
								dataRow.SetValue(i, cTable.GetCell(col, rowIndex));
							}
						}
						dataRow.SetToDefault(context);
						// Note we use a low level 'AddRow' method on the master table
						// here.  This does not touch the table indexes.  The indexes are
						// built later.
						int newRowNumber = newMasterTable.AddRow(dataRow);
						// Set the record as committed added
						newMasterTable.WriteRecordType(newRowNumber, 0x010);
					}
				} catch (DatabaseException e) {
					Logger.Error(this, e);
					throw new Exception(e.Message, e);
				}

				// PENDING: We need to copy any existing index definitions that might
				//   have been set on the table being altered.

				// Rebuild the indexes in the new master table,
				newMasterTable.BuildIndexes();

				// Get the snapshot index set on the new table and set it here
				SetIndexSetForTable(newMasterTable, newMasterTable.CreateIndexSet());

				// Flush this out of the table cache
				FlushTableCache(tableName);

				// Ensure the native sequence generator exists...
				SequenceManager.RemoveNativeTableGenerator(this, tableName);
				SequenceManager.AddNativeTableGenerator(this, tableName);

				// Notify that this database object has been successfully dropped and
				// created.
				OnDatabaseObjectDropped(tableName);
				OnDatabaseObjectCreated(tableName);
			} catch (IOException e) {
				Logger.Error(this, e);
				throw new Exception(e.Message, e);
			}
		}

		/// <summary>
		/// Alters the table with the given name within this transaction to the
		/// specified table definition.
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="tableInfo"></param>
		/// <remarks>
		/// This should only be called under an exclusive lock on the connection.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the table does not exist.
		/// </exception>
		public void AlterTable(TableName tableName, DataTableInfo tableInfo) {
			// Make sure we remember the current sector size of the altered table so
			// we can create the new table with the original size.
			try {
				// HACK: We use index sector size of 2043 for all altered tables
				AlterTable(tableName, tableInfo, -1, 2043);

			} catch (IOException e) {
				throw new Exception("IO Error: " + e.Message);
			}
		}

		/// <summary>
		/// Checks all the rows in the table for immediate constraint violations
		/// and when the transaction is next committed check for all deferred
		/// constraint violations.
		/// </summary>
		/// <param name="tableName"></param>
		/// <remarks>
		/// This method is used when the constraints on a table changes and we
		/// need to determine if any constraint violations occurred. To the 
		/// constraint checking system, this is like adding all the rows to 
		/// the given table.
		/// </remarks>
		public void CheckAllConstraints(TableName tableName) {
			// Get the table
			ITableDataSource table = GetTable(tableName);
			// Get all the rows in the table
			int[] rows = new int[table.RowCount];
			IRowEnumerator rowEnum = table.GetRowEnumerator();
			int i = 0;
			while (rowEnum.MoveNext()) {
				rows[i] = rowEnum.RowIndex;
				++i;
			}
			// Check the constraints of all the rows in the table.
			TableDataConglomerate.CheckAddConstraintViolations(this, table, rows, ConstraintDeferrability.InitiallyImmediate);

			// Add that we altered this table in the journal
			MasterTableDataSource master = FindVisibleTable(tableName, false);
			if (master == null)
				throw new StatementException("Table '" + tableName + "' doesn't exist.");

			// Log in the journal that this transaction touched the table_id.
			int tableId = master.TableId;

			journal.EntryAddTouchedTable(tableId);
			// Log in the journal that we dropped this table.
			journal.EntryTableConstraintAlter(tableId);

		}

		/// <summary>
		///  Compacts the table with the given name within this transaction.
		/// </summary>
		/// <param name="tableName"></param>
		/// <exception cref="StatementException">
		/// If the table doesn't exist.
		/// </exception>
		public void CompactTable(TableName tableName) {
			// Find the master table.
			MasterTableDataSource currentTable = FindVisibleTable(tableName, false);
			if (currentTable == null)
				throw new StatementException("Table '" + tableName + "' doesn't exist.");

			// If the table is worth compacting, or the table is a
			// V1MasterTableDataSource
			if (currentTable.Compact) {
				// The view of this table within this transaction.
				IIndexSet indexSet = GetIndexSetForTable(currentTable);
				// Drop the current table
				DropTable(tableName);
				// And copy to the new table
				CopyTable(currentTable, indexSet);
			}

		}
	}
}