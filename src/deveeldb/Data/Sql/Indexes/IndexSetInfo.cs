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
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Indexes {
	public sealed class IndexSetInfo {
		private IndexSetInfo(ObjectName tableName, IEnumerable<IndexInfo> indexes, bool readOnly) {
			TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
			IsReadOnly = readOnly;
			Indexes = new IndexList(this, indexes);
		}

		public IndexSetInfo(ObjectName tableName)
			: this(tableName, null, false) {
		}

		public ObjectName TableName { get; }

		public bool IsReadOnly { get; }

		public IIndexList Indexes { get; }

		private void AssertNotReadOnly() {
			if (IsReadOnly)
				throw new ArgumentException("The set is read-only.");
		}

		public int FindIndexForColumns(string[] columnNames) {
			int sz = Indexes.Count;

			for (int i = 0; i < sz; ++i) {
				var columns = Indexes[i].ColumnNames;

				if (columns.Length == columnNames.Length) {
					bool passed = true;

					for (int n = 0; n < columns.Length && passed; ++n) {
						if (!columns[n].Equals(columnNames[n])) {
							passed = false;
						}
					}

					if (passed) {
						return i;
					}
				}
			}

			return -1;
		}


		public IndexSetInfo AsReadOnly() {
			return new IndexSetInfo(TableName, Indexes, true);
		}

		#region IndexList

		class IndexList : IIndexList {
			private readonly List<IndexInfo> indexes;
			private readonly IndexSetInfo indexSetInfo;

			public IndexList(IndexSetInfo indexSetInfo, IEnumerable<IndexInfo> indexes) {
				this.indexSetInfo = indexSetInfo;

				this.indexes = new List<IndexInfo>();

				if (indexes != null)
					this.indexes.AddRange(indexes);
			}

			private void AssertMatchesTable(IndexInfo indexInfo) {
				if (indexInfo.TableName != null &&
				    !indexInfo.TableName.Equals(indexSetInfo.TableName))
					throw new ArgumentException($"The index is referencing the table '{indexInfo.TableName}' that is not in scope for the set");
			}

			public IEnumerator<IndexInfo> GetEnumerator() {
				return indexes.GetEnumerator();
			}

			IEnumerator IEnumerable.GetEnumerator() {
				return GetEnumerator();
			}

			public void Add(IndexInfo item) {
				indexSetInfo.AssertNotReadOnly();
				AssertMatchesTable(item);

				indexes.Add(item);
			}

			public void Clear() {
				indexSetInfo.AssertNotReadOnly();
				indexes.Clear();
			}

			public bool Contains(IndexInfo item) {
				if (item == null) throw new ArgumentNullException(nameof(item));

				return Contains(item.IndexName);
			}

			public void CopyTo(IndexInfo[] array, int arrayIndex) {
				throw new NotImplementedException();
			}

			public bool Remove(IndexInfo item) {
				if (item == null) throw new ArgumentNullException(nameof(item));

				indexSetInfo.AssertNotReadOnly();

				return Remove(item.IndexName);
			}

			public int Count => indexes.Count;

			public bool IsReadOnly => indexSetInfo.IsReadOnly;

			public int IndexOf(IndexInfo item) {
				if (item == null) throw new ArgumentNullException(nameof(item));

				return IndexOf(item.IndexName);
			}

			public void Insert(int index, IndexInfo item) {
				indexSetInfo.AssertNotReadOnly();
				AssertMatchesTable(item);

				throw new NotImplementedException();
			}

			public void RemoveAt(int index) {
				indexSetInfo.AssertNotReadOnly();
				indexes.RemoveAt(index);
			}

			public IndexInfo this[int index] {
				get => indexes[index];
				set {
					indexSetInfo.AssertNotReadOnly();
					AssertMatchesTable(value);
					indexes[index] = value;
				}
			}

			public IndexInfo this[string indexName] {
				get => this[IndexOf(indexName)];
				set => this[IndexOf(indexName)] = value;
			}

			public int IndexOf(string indexName) {
				return indexes.FindIndex(x =>
					String.Equals(x.IndexName, indexName, StringComparison.OrdinalIgnoreCase));
			}

			public bool Contains(string indexName) {
				return IndexOf(indexName) != -1;
			}

			public bool Remove(string indexName) {
				indexSetInfo.AssertNotReadOnly();

				var index = IndexOf(indexName);

				if (index == -1)
					return false;

				RemoveAt(index);

				return true;
			}
		}

		#endregion
	}
}