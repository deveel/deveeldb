// 
//  Copyright 2010-2018 Deveel
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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Indexes;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Transactions {
	public sealed class TransactionState : IDisposable {
		private List<IMutableTable> accessedTables;
		private List<ITableSource> selectedTables;
		private Dictionary<ObjectName, ITableSource> visibleTables;
		private readonly Dictionary<ObjectName, IRowIndexSet> tableIndices;

		private List<object> cleanupQueue;

		private bool disposed;

		public TransactionState(ITableSource[] visibleTables, IRowIndexSet[] indexSets) {
			accessedTables = new List<IMutableTable>();
			this.visibleTables = new Dictionary<ObjectName, ITableSource>();
			tableIndices = new Dictionary<ObjectName, IRowIndexSet>();
			selectedTables = new List<ITableSource>();

			if (visibleTables != null) {
				for (int i = 0; i < visibleTables.Length; i++) {
					var tableName = visibleTables[i].TableInfo.TableName;
					this.visibleTables[tableName] = visibleTables[i];
					tableIndices[tableName] = indexSets[i];
				}
			}
		}

		public IEnumerable<ITableSource> SelectedTables {
			get {
				lock (selectedTables) {
					return selectedTables;
				}
			}
		}

		public IEnumerable<IMutableTable> AccessedTables {
			get {
				lock (accessedTables) {
					return accessedTables;
				}
			}
		}

		public IEnumerable<ObjectName> VisibleTables => visibleTables.Keys;

		internal bool TryGetVisibleTable(ObjectName tableName, out ITableSource source)
			=> visibleTables.TryGetValue(tableName, out source);

		internal bool IsTableVisible(ObjectName tableName)
			=> visibleTables.ContainsKey(tableName);

		internal bool TryGetRowIndexSet(ObjectName tableName, out IRowIndexSet indexSet)
			=> tableIndices.TryGetValue(tableName, out indexSet);

		internal void SelectTable(ITableSource source) {
			lock (selectedTables) {
				selectedTables.Add(source);
			}
		}

		internal void AccessTable(IMutableTable table) {
			lock (accessedTables) {
				accessedTables.Add(table);
			}
		}

		internal void AddVisibleTable(ITableSource source, IRowIndexSet indexSet) {
			var tableName = source.TableInfo.TableName;
			visibleTables[tableName] = source;
			tableIndices[tableName] = indexSet;
		}

		internal void AddVisibleTables(ITableSource[] sources, IRowIndexSet[] indexSets) {
			for (int i = 0; i < sources.Length; i++) {
				AddVisibleTable(sources[i], indexSets[i]);
			}
		}

		internal void RemoveVisibleTable(ITableSource source) {
			// TODO: verify if the transaction is read-only

			var tableName = source.TableInfo.TableName;
			if (visibleTables.Remove(tableName)) {
				if (!tableIndices.TryGetValue(tableName, out var indexSet))
					throw new InvalidOperationException("No index set was defined for table.");

				tableIndices.Remove(tableName);

				if (cleanupQueue == null)
					cleanupQueue = new List<object>();

				cleanupQueue.Add(source);
				cleanupQueue.Add(indexSet);
			}
		}

		internal void UpdateVisibleTable(ITableSource table, IRowIndexSet indexSet) {
			// TODO: verify if the transaction is read-only

			RemoveVisibleTable(table);
			AddVisibleTable(table, indexSet);
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
				// TODO: transaction.RaiseError(ex);
			} finally {
				accessedTables = null;
			}
		}

		private void DisposeAllIndices() {
			// Dispose all the IIndexSet for each table
			try {
				if (tableIndices != null) {
					foreach (var tableIndex in tableIndices.Values) {
						tableIndex.Dispose();
					}

					tableIndices.Clear();
				}
			} catch (Exception ex) {
				// TODO: ? Transaction.OnError(ex);
			}

			// Dispose all tables we dropped (they will be in the cleanup_queue.
			try {
				if (cleanupQueue != null) {
					foreach (var indexSet in cleanupQueue.OfType<IRowIndexSet>()) {
						indexSet.Dispose();
					}

					cleanupQueue.Clear();
				}
			} catch (Exception ex) {
				// TODO: ? Transaction.OnError(ex);
			} finally { 
				cleanupQueue = null;
			}
		}

		public void Dispose() {
			if (!disposed) {
				DisposeTouchedTables();
				DisposeAllIndices();
			}


			disposed = true;
		}
	}
}