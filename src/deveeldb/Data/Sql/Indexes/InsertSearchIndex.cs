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

using Deveel.Collections;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Indexes {
	public sealed class InsertSearchIndex : CollatedSearchIndex {
		private ISortedCollection<IndexKey, long> list;
		private ISortComparer<IndexKey, long> comparer;

		public InsertSearchIndex(IndexInfo indexInfo, ITable table) 
			: this(indexInfo, table, null) {
		}

		public InsertSearchIndex(IndexInfo indexInfo, ITable table, IEnumerable<long> rows) 
			: base(indexInfo, table) {
			comparer = new IndexComparer(this);
			list = new SortedCollection<IndexKey, long>();

			if (rows != null) {
				foreach (var row in rows) {
					list.Add(row);
				}
			}
		}

		private InsertSearchIndex(InsertSearchIndex source, bool readOnly)
			: this(source.IndexInfo, source.Table, null) {
			IsReadOnly = readOnly;
		}

		public override bool IsReadOnly { get; }

		protected override long RowCount => list.Count;

		protected override IndexKey First => GetKey(list[0]);

		protected override IndexKey Last => GetKey(list[list.Count - 1]);

		public override void Insert(long row) {
			ThrowIfReadOnly();

			var value = GetKey(row);
			list.InsertSort(value, row, comparer);
		}

		public override void Remove(long row) {
			ThrowIfReadOnly();

			var value = GetKey(row);
			var removed = list.RemoveSort(value, row, comparer);

			if (removed != row)
				throw new InvalidOperationException($"Could not remove the requested row ({row})");
		}

		protected override IEnumerable<long> AddRange(long start, long end, IEnumerable<long> input) {
			var result = new BigList<long>();
			if (input != null)
				result.AddRange(input);

			using (var en = list.GetEnumerator(start, end)) {
				while (en.MoveNext()) {
					result.Add(en.Current);
				}
			}

			return result;
		}

		protected override long SearchFirst(IndexKey value) {
			return list.SearchFirst(value, comparer);
		}

		protected override long SearchLast(IndexKey value) {
			return list.SearchLast(value, comparer);
		}

		#region IndexComparerImpl

		private class IndexComparer : ISortComparer<IndexKey, long> {
			private readonly InsertSearchIndex columnIndex;

			public IndexComparer(InsertSearchIndex columnIndex) {
				this.columnIndex = columnIndex;
			}

			private int InternalCompare(long index, IndexKey value) {
				var key = columnIndex.GetKey(index);
				return key.CompareTo(value);
			}

			public int Compare(long index, IndexKey val) {
				return InternalCompare(index, val);
			}
		}

		#endregion
	}
}