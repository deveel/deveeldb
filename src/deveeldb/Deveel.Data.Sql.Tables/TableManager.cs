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
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Diagnostics;
using Deveel.Data.Index;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Transactions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Tables {
	public sealed class TableManager : IObjectManager {
		private readonly List<ITableSource> visibleTables;
		private List<IMutableTable> accessedTables;
		private List<ITableSource> selectedTables;
		private readonly List<IIndexSet> tableIndices;
		private List<ITableContainer> internalTables;

		private readonly Dictionary<ObjectName, IMutableTable> tableCache;

		private List<object> cleanupQueue;

		public TableManager(ITransaction transaction, ITableSourceComposite composite) {
			if (transaction == null)
				throw new ArgumentNullException("transaction");

			Transaction = transaction;

			Composite = composite;

			visibleTables = new List<ITableSource>();
			tableIndices = new List<IIndexSet>();
			accessedTables = new List<IMutableTable>();
			tableCache = new Dictionary<ObjectName, IMutableTable>();
			selectedTables = new List<ITableSource>();
		}

		~TableManager() {
			Dispose(true);
		}

		public ITransaction Transaction { get; private set; }

		internal ITableSourceComposite Composite { get; set; }

		internal IEnumerable<IMutableTable> AccessedTables {
			get { return accessedTables; }
		}

		internal IEnumerable<ITableSource> SelectedTables {
			get {
				lock (selectedTables) {
					return selectedTables.ToArray();
				}
			}
		}

		private bool IgnoreIdentifiersCase {
			get { return Transaction.IgnoreIdentifiersCase(); }
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void DisposeAllIndices() {
			// Dispose all the IIndexSet for each table
			try {
				if (tableIndices != null) {
					foreach (var tableIndex in tableIndices) {
						tableIndex.Dispose();
					}

					tableIndices.Clear();
				}
			} catch (Exception ex) {
				Transaction.AsEventSource().OnError(ex);
			}

			// Dispose all tables we dropped (they will be in the cleanup_queue.
			try {
				if (cleanupQueue != null) {
					for (int i = 0; i < cleanupQueue.Count; i += 2) {
						var tableSource = (TableSource) cleanupQueue[i];
						IIndexSet indexSet = (IIndexSet) cleanupQueue[i + 1];
						indexSet.Dispose();
					}

					cleanupQueue.Clear();
				}
			} catch (Exception ex) {
				Transaction.AsEventSource().OnError(ex);
			} finally { 
				cleanupQueue = null;
			}
		}

		private void DisposeTouchedTables() {
			try {
				if (accessedTables != null) {
					foreach (var table in accessedTables) {
						table.Dispose();
					}

					accessedTables.Clear();
				}
			} catch (Exception ex) {
				Transaction.AsEventSource().OnError(ex);
			} finally {
				accessedTables = null;
			}
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				DisposeAllIndices();
				DisposeTouchedTables();
			}

			Transaction = null;
		}

		DbObjectType IObjectManager.ObjectType {
			get { return DbObjectType.Table; }
		}

		void IObjectManager.CreateObject(IObjectInfo objInfo) {
			var tableInfo = objInfo as TableInfo;
			if (tableInfo == null)
				throw new ArgumentException();

			CreateTable(tableInfo);
		}

		public void CreateTable(TableInfo tableInfo) {
			CreateTable(tableInfo, false);
		}

		public void CreateTable(TableInfo tableInfo, bool temporary) {
			var tableName = tableInfo.TableName;
			var source = FindVisibleTable(tableName, false);
			if (source != null)
				throw new InvalidOperationException(String.Format("Table '{0}' already exists.", tableName));

			tableInfo = tableInfo.AsReadOnly();

			source = Composite.CreateTableSource(tableInfo, temporary);

			// Add this table (and an index set) for this table.
			AddVisibleTable(source, source.CreateIndexSet());

			int tableId = source.TableId;
			Transaction.OnTableCreated(tableId, tableName);

			Transaction.CreateNativeSequence(tableName);
		}

		public void CreateTemporaryTable(TableInfo tableInfo) {
			CreateTable(tableInfo, true);
		}

		public void SelectTable(ObjectName tableName) {
			// Special handling of internal tables,
			if (IsDynamicTable(tableName))
				return;

			var source = FindVisibleTable(tableName, false);
			if (source == null)
				throw new ObjectNotFoundException(tableName);

			lock (selectedTables) {
				if (!selectedTables.Contains(source))
					selectedTables.Add(source);
			}
		}

		public void CompactTable(ObjectName tableName) {
			// Find the master table.
			var currentTable = FindVisibleTable(tableName, false);
			if (currentTable == null)
				throw new ObjectNotFoundException(tableName);

			// If the table is worth compacting
			if (currentTable.CanCompact) {
				// The view of this table within this transaction.
				var indexSet = GetIndexSetForTable(currentTable);

				// Drop the current table
				DropTable(tableName);

				// And copy to the new table
				CopyTable(currentTable, indexSet);
			}
		}

		private void CopyTable(ITableSource tableSource, IIndexSet indexSet) {
			var tableInfo = tableSource.TableInfo;
			var tableName = tableInfo.TableName;
			var source = FindVisibleTable(tableName, false);
			if (source != null)
				throw new ObjectNotFoundException(tableName);

			// Copy the table and add to the list of visible tables.
			source = Composite.CopySourceTable(tableSource, indexSet);

			AddVisibleTable(source, source.CreateIndexSet());

			// Log in the journal that this transaction touched the table_id.
			int tableId = source.TableId;
			Transaction.OnTableCreated(tableId, tableName);

			Transaction.CreateNativeSequence(tableName);
		}

		internal IIndexSet GetIndexSetForTable(ITableSource tableSource) {
			int sz = tableIndices.Count;
			for (int i = 0; i < sz; ++i) {
				if (visibleTables[i].TableId == tableSource.TableId) {
					return tableIndices[i];
				}
			}

			throw new Exception("Table source not found in this transaction.");
		}

		private void AddVisibleTable(ITableSource table, IIndexSet indexSet) {
			if (Transaction.ReadOnly())
				throw new Exception("Transaction is Read-only.");

			visibleTables.Add(table);
			tableIndices.Add(indexSet);
		}

		private static int IndexOfTable(IList<ITableSource> sources, int tableId) {
			for (int i = 0; i < sources.Count; i++) {
				var source = sources[i];
				if (source.TableId == tableId)
					return i;
			}

			return -1;
		}

		internal void RemoveVisibleTable(ITableSource table) {
			if (Transaction.ReadOnly())
				throw new Exception("Transaction is Read-only.");

			var i = IndexOfTable(visibleTables, table.TableId);
			if (i != -1) {
				visibleTables.RemoveAt(i);
				IIndexSet indexSet = tableIndices[i];
				tableIndices.RemoveAt(i);
				if (cleanupQueue == null)
					cleanupQueue = new List<object>();

				cleanupQueue.Add(table);
				cleanupQueue.Add(indexSet);

				// Remove from the table cache
				var tableName = table.TableInfo.TableName;
				tableCache.Remove(tableName);
			}
		}

		internal void UpdateVisibleTable(TableSource table, IIndexSet indexSet) {
			if (Transaction.ReadOnly())
				throw new Exception("Transaction is Read-only.");

			RemoveVisibleTable(table);
			AddVisibleTable(table, indexSet);
		}

		private ITableSource FindVisibleTable(ObjectName tableName, bool ignoreCase) {
			return visibleTables
				.FirstOrDefault(source => source != null &&
				                          source.TableInfo.TableName.Equals(tableName, ignoreCase));
		}

		public SqlNumber SetUniqueId(ObjectName tableName, SqlNumber value) {
			var tableSource = FindVisibleTable(tableName, false);
			if (tableSource == null)
				throw new ObjectNotFoundException(tableName,
					String.Format("Table with name '{0}' could not be found to set the unique id.", tableName));

			tableSource.SetUniqueId(value.ToInt64());
			return value;
		}

		public SqlNumber NextUniqueId(ObjectName tableName) {
			var tableSource = FindVisibleTable(tableName, false);
			if (tableSource == null)
				throw new ObjectNotFoundException(tableName,
					String.Format("Table with name '{0}' could not be found to retrieve unique id.", tableName));

			var value = tableSource.GetNextUniqueId();
			return new SqlNumber(value);
		}

		private bool IsDynamicTable(ObjectName tableName) {
			if (internalTables == null)
				return false;

			return internalTables.Any(info => info != null && info.ContainsTable(tableName));
		}

		private ITable GetDynamicTable(ObjectName tableName) {
			foreach (var info in internalTables) {
				if (info != null) {
					int index = info.FindByName(tableName);
					if (index != -1)
						return info.GetTable(index);
				}
			}

			throw new ArgumentException(String.Format("Table '{0}' is not a dynamic table.", tableName));
		}

		private string GetDynamicTableType(ObjectName tableName) {
			// Otherwise we need to look up the table in the internal table list,
			foreach (var info in internalTables) {
				if (info != null) {
					int index = info.FindByName(tableName);
					if (index != -1)
						return info.GetTableType(index);
				}
			}

			throw new ArgumentException(String.Format("Table '{0}' is not a dynamic table.", tableName));
		}

		bool IObjectManager.RealObjectExists(ObjectName objName) {
			return RealTableExists(objName);
		}

		public bool RealTableExists(ObjectName tableName) {
			return FindVisibleTable(tableName, false) != null;
		}

		bool IObjectManager.ObjectExists(ObjectName objName) {
			return TableExists(objName);
		}

		public bool TableExists(ObjectName tableName) {
			return IsDynamicTable(tableName) ||
			       RealTableExists(tableName);
		}

		IDbObject IObjectManager.GetObject(ObjectName objName) {
			return GetTable(objName);
		}

		public ITable GetTable(ObjectName tableName) {
			// If table is in the cache, return it
			IMutableTable table;
			if (tableCache.TryGetValue(tableName, out table))
				return table;

			var source = FindVisibleTable(tableName, false);

			if (source == null) {
				if (IsDynamicTable(tableName))
					return GetDynamicTable(tableName);
			} else {
				// Otherwise make a view of tha master table data source and write it in
				// the cache.
				table = CreateTableAtCommit(source);

				// Put table name in the cache
				tableCache[tableName] = table;
			}

			return table;
		}

		public String GetTableType(ObjectName tableName) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");

			if (IsDynamicTable(tableName))
				return GetDynamicTableType(tableName);
			if (FindVisibleTable(tableName, false) != null)
				return TableTypes.Table;

			// No table found so report the error.
			throw new ObjectNotFoundException(tableName);
		}

		private ObjectName[] GetDynamicTables() {
			int sz = internalTables.Where(container => container != null).Sum(container => container.TableCount);

			var list = new ObjectName[sz];
			int index = -1;

			foreach (var container in internalTables) {
				if (container != null) {
					int tableCount = container.TableCount;
					for (int i = 0; i < tableCount; ++i) {
						list[++index] = container.GetTableName(i);
					}
				}
			}

			return list;
		}

		public ObjectName TryResolveCase(ObjectName tableName) {
			// Is it a visable table (match case insensitive)
			var table = FindVisibleTable(tableName, true);
			if (table != null)
				return table.TableInfo.TableName;

			var comparison = IgnoreIdentifiersCase
				? StringComparison.OrdinalIgnoreCase
				: StringComparison.Ordinal;

			// Is it an internal table?
			string tschema = tableName.ParentName;
			string tname = tableName.Name;
			var list = GetDynamicTables();
			foreach (var ctable in list) {
				if (String.Equals(ctable.ParentName, tschema, comparison) &&
				    String.Equals(ctable.Name, tname, comparison)) {
					return ctable;
				}
			}

			// No matches so return the original object.
			return tableName;
		}

		public IMutableTable GetMutableTable(ObjectName tableName) {
			var table = GetTable(tableName);
			if (table == null)
				return null;

			if (!(table is IMutableTable))
				throw new InvalidOperationException();

			return (IMutableTable) table;
		}

		private TableInfo GetDynamicTableInfo(ObjectName tableName) {
			foreach (var info in internalTables) {
				if (info != null) {
					int index = info.FindByName(tableName);
					if (index != -1)
						return info.GetTableInfo(index);
				}
			}

			throw new Exception("Not an internal table: " + tableName);
		}

		public TableInfo GetTableInfo(ObjectName tableName) {
			// If this is a dynamic table then handle specially
			if (IsDynamicTable(tableName))
				return GetDynamicTableInfo(tableName);

			// Otherwise return from the pool of visible tables
			return visibleTables
				.Select(table => table.TableInfo)
				.FirstOrDefault(tableInfo => tableInfo.TableName.Equals(tableName));
		}

		private IMutableTable CreateTableAtCommit(ITableSource source) {
			// Create the table for this transaction.
			var table = source.CreateTableAtCommit(Transaction);

			accessedTables.Add(table);

			Transaction.OnTableAccessed(source.TableId, source.TableInfo.TableName);

			return table;
		}

		bool IObjectManager.AlterObject(IObjectInfo objInfo) {
			var tableInfo = objInfo as TableInfo;
			if (tableInfo == null)
				throw new ArgumentException();

			return AlterTable(tableInfo);
		}

		public bool AlterTable(TableInfo tableInfo) {
			tableInfo = tableInfo.AsReadOnly();

			var tableName = tableInfo.TableName;

			// The current schema context is the schema of the table name
			string currentSchema = tableName.Parent.Name;
			using (var session = new SystemSession(Transaction, currentSchema)) {
				using (var context = session.CreateQuery()) {

					// Get the next unique id of the unaltered table.
					var nextId = NextUniqueId(tableName);

					// Drop the current table
					var cTable = GetTable(tableName);
					var droppedTableId = cTable.TableInfo.Id;

					DropTable(tableName);

					// And create the table table
					CreateTable(tableInfo);

					var alteredTable = GetMutableTable(tableName);
					var source = FindVisibleTable(tableName, false);
					int alteredTableId = source.TableId;

					// Set the sequence id of the table
					source.SetUniqueId(nextId.ToInt64());

					// Work out which columns we have to copy to where
					int[] colMap = new int[tableInfo.ColumnCount];
					var origTd = cTable.TableInfo;
					for (int i = 0; i < colMap.Length; ++i) {
						string colName = tableInfo[i].ColumnName;
						colMap[i] = origTd.IndexOfColumn(colName);
					}

					// First move all the rows from the old table to the new table,
					// This does NOT update the indexes.
					var e = cTable.GetEnumerator();
					while (e.MoveNext()) {
						int rowIndex = e.Current.RowId.RowNumber;
						var dataRow = alteredTable.NewRow();
						for (int i = 0; i < colMap.Length; ++i) {
							int col = colMap[i];
							if (col != -1) {
								dataRow.SetValue(i, cTable.GetValue(rowIndex, col));
							}
						}

						dataRow.SetDefault(context);

						// Note we use a low level 'AddRow' method on the master table
						// here.  This does not touch the table indexes.  The indexes are
						// built later.
						int newRowNumber = source.AddRow(dataRow);

						// Set the record as committed added
						source.WriteRecordState(newRowNumber, RecordState.CommittedAdded);
					}

					// TODO: We need to copy any existing index definitions that might
					//   have been set on the table being altered.

					// Rebuild the indexes in the new master table,
					source.BuildIndexes();

					// Get the snapshot index set on the new table and set it here
					SetIndexSetForTable(source, source.CreateIndexSet());

					// Flush this out of the table cache
					FlushTableCache(tableName);

					// Ensure the native sequence generator exists...
					Transaction.RemoveNativeSequence(tableName);
					Transaction.CreateNativeSequence(tableName);

					// Notify that this database object has been successfully dropped and
					// created.
					Transaction.OnTableDropped(droppedTableId, tableName);
					Transaction.OnTableCreated(alteredTableId, tableName);

					return true;
				}
			}
		}

		private
			void FlushTableCache(ObjectName tableName) {
			tableCache.Remove(tableName);
		}

		private void SetIndexSetForTable(ITableSource source, IIndexSet indexSet) {
			int sz = tableIndices.Count;
			for (int i = 0; i < sz; ++i) {
				if (visibleTables[i].TableId == source.TableId) {
					tableIndices[i] = indexSet;
					return;
				}
			}

			throw new Exception("Table source not found in this transaction.");
		}

		bool IObjectManager.DropObject(ObjectName objName) {
			return DropTable(objName);
		}

		public ObjectName ResolveName(ObjectName objName, bool ignoreCase) {
			// Is it a visible table (match case insensitive)
			var table = FindVisibleTable(objName, ignoreCase);
			if (table != null)
				return table.TableInfo.TableName;

			// Is it an internal table?
			string tschema = objName.ParentName;
			string tname = objName.Name;
			var list = GetDynamicTables();

			var comparison = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
			foreach (var ctable in list) {
				if (String.Equals(ctable.ParentName, tschema, comparison) &&
				    String.Equals(ctable.Name, tname, comparison)) {
					return ctable;
				}
			}

			// No matches so return the original object.
			return null;
		}

		public bool DropTable(ObjectName tableName) {
			var source = FindVisibleTable(tableName, false);
			if (source == null)
				return false;

			// Removes this table from the visible table list of this transaction
			RemoveVisibleTable(source);

			// Log in the journal that this transaction touched the table_id.
			int tableId = source.TableId;
			Transaction.OnTableDropped(tableId, tableName);

			Transaction.RemoveNativeSequence(tableName);

			return true;
		}

		public void AssertConstraints(ObjectName tableName) {
			var table = Transaction.GetTable(tableName);

			// Get all the rows in the table
			int[] rows = table.Select(row => row.RowId.RowNumber).ToArray();

			// Check the constraints of all the rows in the table.
			Transaction.CheckAddConstraintViolations(table, rows, ConstraintDeferrability.InitiallyImmediate);

			// Add that we altered this table in the journal
			var master = FindVisibleTable(tableName, false);
			if (master == null)
				throw new InvalidOperationException("Table '" + tableName + "' doesn't exist.");

			// Log in the journal that this transaction touched the table_id.
			int tableId = master.TableId;

			Transaction.OnTableAccessed(tableId, tableName);

			// Log in the journal that we dropped this table.
			Transaction.OnTableConstraintAltered(tableId);
		}

		public void AddInternalTables(ITableContainer container) {
			if (internalTables == null)
				internalTables = new List<ITableContainer>();

			internalTables.Add(container);
		}

		internal IEnumerable<ITableSource> GetVisibleTables() {
			return visibleTables.ToArray();
		}

		internal void AddVisibleTables(IEnumerable<TableSource> tableSources, IEnumerable<IIndexSet> indexSets) {
			var tableList = tableSources.ToList();
			var indexSetList = indexSets.ToList();
			for (int i = 0; i < tableList.Count; i++) {
				AddVisibleTable(tableList[i], indexSetList[i]);
			}
		}

		public IEnumerable<ObjectName> GetTableNames() {
			var result = (visibleTables
				.Where(tableSource => tableSource != null)
				.Select(tableSource => tableSource.TableInfo.TableName)).ToList();

			var dynamicTables = GetDynamicTables();
			if (dynamicTables != null)
				result.AddRange(dynamicTables);

			return result.ToArray();
		}
	}
}