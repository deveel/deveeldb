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

using Deveel.Data.Events;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Transactions {
	public sealed class TransactionState : IDisposable {
		private List<ITable> accessedTables;
		private List<ITableSource> selectedTables;
		private Dictionary<ObjectName, ITableSource> visibleTables;
		private bool disposed;

		public TransactionState(IEnumerable<ITableSource> visibleTables) {
			accessedTables = new List<ITable>();
			this.visibleTables = new Dictionary<ObjectName, ITableSource>();
			selectedTables = new List<ITableSource>();

			if (visibleTables != null) {
				foreach (var table in visibleTables) {
					this.visibleTables[table.TableInfo.TableName] = table;
				}
			}
		}

		public IEnumerable<int> SelectedTables {
			get {
				lock (selectedTables) {
					return selectedTables.Select(x => x.TableId);
				}
			}
		}

		public IEnumerable<ITable> AccessedTables {
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

		internal void SelectTable(ITableSource source) {
			lock (selectedTables) {
				selectedTables.Add(source);
			}
		}

		internal void AccessTable(ITable table) {
			lock (accessedTables) {
				accessedTables.Add(table);
			}
		}

		internal void AddVisibleTable(ITableSource source) {
			visibleTables[source.TableInfo.TableName] = source;
		}

		internal void RemoveVisibleTable(ITableSource source) {
			// TODO: verify if the transaction is read-only

			var tableName = source.TableInfo.TableName;
			if (visibleTables.Remove(tableName)) {
				// TODO:
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
				// TODO: transaction.RaiseError(ex);
			} finally {
				accessedTables = null;
			}
		}

		public void Dispose() {
			if (!disposed)
				DisposeTouchedTables();

			disposed = true;
		}
	}
}