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

using Deveel.Data.Sql;

namespace Deveel.Data.Index {
	public sealed class InsertSearchIndex : CollatedSearchIndex {
		private IIndex<int> list;
		private IIndexComparer<int> comparer;

		private bool recordUid;

		private readonly bool readOnly;
		private readonly int readOnlyCount;

		public InsertSearchIndex(ITable table, int columnOffset) 
			: base(table, columnOffset) {
			comparer = new IndexComparerImpl(this);
			list = new BlockIndex<int>();
		}

		public InsertSearchIndex(ITable table, int columnOffset, IEnumerable<int> list)
			: this(table, columnOffset) {
			if (list != null) {
				foreach (var item in list) {
					this.list.Add(item);
				}
			}
		}

		private InsertSearchIndex(ITable table, InsertSearchIndex source, bool readOnly)
			: this(table, source.ColumnOffset, source.list) {
			this.readOnly = readOnly;

			if (readOnly)
				readOnlyCount = list.Count;

			// Do we generate lookup caches?
			recordUid = source.recordUid;

		}

		public override bool IsReadOnly {
			get { return readOnly; }
		}

		protected override int Count {
			get { return list.Count; }
		}

		public override string IndexType {
			get { return DefaultIndexTypes.InsertSearch; }
		}

		protected override DataObject First {
			get { return GetValue(list[0]); }
		}

		protected override DataObject Last {
			get { return GetValue(list[list.Count - 1]); }
		}

		internal bool RecordUid { get; set; }

		public override void Insert(int rowNumber) {
			if (IsReadOnly)
				throw new InvalidOperationException("Tried to change an read-only index.");

			var value = GetValue(rowNumber);
			list.InsertSort(value, rowNumber, comparer);
		}

		public override void Remove(int rowNumber) {
			if (IsReadOnly)
				throw new InvalidOperationException("Tried to change an read-only index.");

			var value = GetValue(rowNumber);
			var removed = list.RemoveSort(value, rowNumber, comparer);

			if (removed != rowNumber)
				throw new InvalidOperationException(String.Format("Could not remove the requested row ({0})", rowNumber));
		}

		protected override IEnumerable<int> AddRange(int start, int end, IEnumerable<int> input) {
			var result = new List<int>();
			if (input != null)
				result.AddRange(input);

			var en = list.GetEnumerator(start, end);
			while (en.MoveNext()) {
				result.Add(en.Current);
			}

			return result.ToArray();
		}

		public override ColumnIndex Copy(ITable table, bool readOnly) {
			// ASSERTION: If readOnly, check the size of the current set is equal to
			//   when the index was created.
			if (IsReadOnly && readOnlyCount != list.Count)
				throw new InvalidOperationException("Assert failed: read-only size is different from when created.");

			// We must create a new InsertSearch object and copy all the state
			// information from this object to the new one.
			return new InsertSearchIndex(table, this, readOnly);
		}

		protected override int SearchFirst(DataObject value) {
			return list.SearchFirst(value, comparer);
		}

		protected override int SearchLast(DataObject value) {
			return list.SearchLast(value, comparer);
		}

		protected override void Dispose(bool disposing) {
			list = null;
			comparer = null;
		}

		#region IndexComparerImpl

		private class IndexComparerImpl : IIndexComparer<int> {
			private readonly InsertSearchIndex columnIndex;

			public IndexComparerImpl(InsertSearchIndex columnIndex) {
				this.columnIndex = columnIndex;
			}

			private int InternalCompare(int index, DataObject value) {
				var cell = columnIndex.GetValue(index);
				var cmp =  cell.CompareTo(value);
				return cmp;
			}

			public int CompareValue(int index, DataObject val) {
				return InternalCompare(index, val);
			}

			public int Compare(int index1, int index2) {
				var cell = columnIndex.GetValue(index2);
				return InternalCompare(index1, cell);
			}
		}

		#endregion
	}
}
