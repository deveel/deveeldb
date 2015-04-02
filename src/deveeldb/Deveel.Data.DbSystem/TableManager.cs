// 
//  Copyright 2010-2015 Deveel
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

using Deveel.Data.Index;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	public sealed class TableManager : ITableManager {
		private readonly List<int> visibleTables;
		private readonly List<IIndexSet> tableIndices;
		private List<ITableContainer> internalTables;

		private readonly Dictionary<ObjectName, ITable> tableCache;
		private readonly Dictionary<ObjectName, SqlNumber> sequenceValueCache; 

		private List<object> cleanupQueue;

		public TableManager(ITransaction transaction)
			: this(transaction, new TableSourceComposite(transaction.Context.SystemContext, transaction.Context.Database)) {
		}

		public TableManager(ITransaction transaction, TableSourceComposite composite) {
			if (transaction == null)
				throw new ArgumentNullException("transaction");

			Transaction = transaction;

			Composite = composite;

			visibleTables = new List<int>();
			tableIndices = new List<IIndexSet>();
			tableCache = new Dictionary<ObjectName, ITable>();
			sequenceValueCache = new Dictionary<ObjectName, SqlNumber>();
		}

		public ITransaction Transaction { get; private set; }

		public TableSourceComposite Composite { get; private set; }

		public void Dispose() {
			
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
			Transaction.Registry.RegisterEvent(new TableCreatedEvent(tableId, tableName));

			Transaction.CreateNativeSequence(tableName);
		}

		public void CreateTemporaryTable(TableInfo tableInfo) {
			CreateTable(tableInfo, true);
		}

		public void SelectTable(ObjectName tableName) {
			
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

		private void CopyTable(TableSource tableSource, IIndexSet indexSet) {
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
			Transaction.Registry.RegisterEvent(new TableCreatedEvent(tableId, tableName));

			Transaction.CreateNativeSequence(tableName);
		}

		internal IIndexSet GetIndexSetForTable(TableSource tableSource) {
			int sz = tableIndices.Count;
			for (int i = 0; i < sz; ++i) {
				if (visibleTables[i] == tableSource.TableId) {
					return tableIndices[i];
				}
			}

			throw new Exception("Table source not found in this transaction.");
		}

		private void AddVisibleTable(TableSource table, IIndexSet indexSet) {
			if (Transaction.IsReadOnly)
				throw new Exception("Transaction is Read-only.");

			visibleTables.Add(table.TableId);
			tableIndices.Add(indexSet);
		}

		private void RemoveVisibleTable(TableSource table) {
			if (Transaction.IsReadOnly)
				throw new Exception("Transaction is Read-only.");

			int i = visibleTables.IndexOf(table.TableId);
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

		private TableSource FindVisibleTable(ObjectName tableName, bool ignoreCase) {
			return visibleTables.Select(tableId => Composite.GetTableSource(tableId))
				.FirstOrDefault(source => source != null &&
				                          source.TableInfo.TableName.Equals(tableName, ignoreCase));
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
			ITable table;
			if (tableCache.TryGetValue(tableName, out table))
				return table;

			var source = FindVisibleTable(tableName, false);

			if (source == null) {
				if (IsDynamicTable(tableName))
					return GetDynamicTable(tableName);
			} else {
				// Otherwise make a view of tha master table data source and write it in
				// the cache.
				table = CreateTableSourceAtCommit(source);

				// Put table name in the cache
				tableCache[tableName] = table;
			}

			return table;
		}

		public IMutableTable GetMutableTable(ObjectName tableName) {
			var table = GetTable(tableName);
			if (table == null)
				return null;

			if (!(table is IMutableTable))
				throw new InvalidOperationException();

			return (IMutableTable) table;
		}

		private ITable CreateTableSourceAtCommit(TableSource source) {
			// Create the table for this transaction.
			var table = source.CreateTableAtCommit(this);

			Transaction.Registry.RegisterEvent(new TableAccessEvent(source.TableId, source.TableInfo.TableName));

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
			var context = new SystemQueryContext(Transaction, currentSchema);

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
			source.SetUniqueId(nextId);

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
				source.WriteRecordType(newRowNumber, TableRecordState.Added);
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
			Transaction.Registry.RegisterEvent(new TableDroppedEvent(droppedTableId, tableName));
			Transaction.Registry.RegisterEvent(new TableCreatedEvent(alteredTableId, tableName));

			return true;
		}

		private void FlushTableCache(ObjectName tableName) {
			tableCache.Remove(tableName);
		}

		private void SetIndexSetForTable(TableSource source, IIndexSet indexSet) {
			int sz = tableIndices.Count;
			for (int i = 0; i < sz; ++i) {
				if (visibleTables[i] == source.TableId) {
					tableIndices[i] = indexSet;
					return;
				}
			}

			throw new Exception("Table source not found in this transaction.");
		}

		bool IObjectManager.DropObject(ObjectName objName) {
			return DropTable(objName);
		}

		public bool DropTable(ObjectName tableName) {
			var source = FindVisibleTable(tableName, false);
			if (source == null)
				return false;

			// Removes this table from the visible table list of this transaction
			RemoveVisibleTable(source);

			// Log in the journal that this transaction touched the table_id.
			int tableId = source.TableId;
			Transaction.Registry.RegisterEvent(new TableDroppedEvent(tableId, tableName));

			Transaction.RemoveNativeSequence(tableName);

			return true;
		}

		public long NextUniqueId(ObjectName tableName) {
			if (Transaction.IsReadOnly)
				throw new Exception("Sequence operation not permitted for read only transaction.");

			var source = FindVisibleTable(tableName, false);
			if (source == null)
				throw new ObjectNotFoundException(tableName);

			return source.GetNextUniqueId();
		}

		public void SetUniqueId(ObjectName tableName, int uniqueId) {
			if (Transaction.IsReadOnly)
				throw new Exception("Sequence operation not permitted for read only transaction.");

			var source = FindVisibleTable(tableName, false);
			if (source == null)
				throw new ObjectNotFoundException(tableName);

			source.SetUniqueId(uniqueId);
		}

		public void AssertConstraints(ObjectName tableName) {
			throw new NotImplementedException();
		}
	}
}
