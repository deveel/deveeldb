// 
//  Copyright 2010-2014 Deveel
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
using System.Linq;

using Deveel.Data.Sql;

namespace Deveel.Data.Index {
	public class SortedIndex : CollatedTableIndex {
		private IIndex<long> index;
		private IIndexComparer<long> comparer;

		private long readOnlyLength;
 
		public SortedIndex(ITable table, int columnOffset) 
			: base(table, columnOffset) {
			index = new BlockIndex<long>();
			comparer = new IndexComparer(this);
		}

		public SortedIndex(ITable table, int columnOffset, IEnumerable<long> values)
			: this(table, columnOffset) {
			foreach (var value in values) {
				index.Add(value);
			}
		}

		public SortedIndex(ITable table, int columnOffset, IIndex<long> index)
			: this(table, columnOffset) {
			this.index = index;
		}

		private SortedIndex(ITable table, SortedIndex source, bool readOnly)
			: base(table, source.ColumnOffset) {
			IsReadOnly = readOnly;

			if (readOnly) {
				// Immutable is a shallow copy
				index = source.index;
				readOnlyLength = index.Count;
			} else {
				index = new BlockIndex<long>(source.index);
			}

			comparer = new IndexComparer(this);
		}

		protected override long Length {
			get { return index.Count; }
		}

		protected override DataObject FirstInOrder {
			get { return GetValue(index[0]); }
		}

		protected override DataObject LastInOrder {
			get { return GetValue(index[(int)Length - 1]); }
		}

		private void AssertNotReadOnly() {
			if (IsReadOnly)
				throw new InvalidOperationException("Cannot mutate a rea-donly index.");
		}

		protected override IEnumerable<long> AddRangeToSet(long start, long end, IEnumerable<long> list) {
			if (list == null)
				list = new long[(end - start) + 2];

			var result = new List<long>(list);
			var en = index.GetEnumerator((int)start, (int)end);
			while (en.MoveNext()) {
				result.Add(en.Current);
			}

			return result.AsReadOnly();
		}

		public override void Insert(long rowNumber) {
			AssertNotReadOnly();

			var value = GetValue(rowNumber);
			index.InsertSort(value, rowNumber, comparer);
		}

		public override void Remove(long rowNumber) {
			AssertNotReadOnly();

			var value = GetValue(rowNumber);
			var removed = index.RemoveSort(value, rowNumber, comparer);

			if (removed != rowNumber)
				throw new InvalidOperationException(
					String.Format("A different row ({0}) was removed from the index " +
					              "rather than the one requested ({1}).", removed, rowNumber));
		}

		public override TableIndex Copy(ITable table, bool readOnly) {
			if (IsReadOnly && (readOnlyLength != index.Count))
				throw new InvalidOperationException("A read-only index was mutated.");
			
			return new SortedIndex(table, this, readOnly);
		}

		protected override long SearchFirst(DataObject value) {
			return index.SearchFirst(value, comparer);
		}

		protected override long SearchLast(DataObject value) {
			return index.SearchLast(value, comparer);
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				index = null;
				comparer = null;
			}

			base.Dispose(disposing);
		}

		public override IEnumerable<long> SelectAll() {
			return index.ToList();
		}

		#region IndexComparer

		class IndexComparer : IIndexComparer<long> {
			private readonly SortedIndex sortedIndex;

			public IndexComparer(SortedIndex sortedIndex) {
				this.sortedIndex = sortedIndex;
			}

			private int DoCompare(long index, DataObject value) {
				var cell = sortedIndex.GetValue(index);
				return cell.CompareTo(value);
			}

			public int CompareValue(long index, DataObject value) {
				return DoCompare(index, value);
			}

			public int Compare(long index1, long index2) {
				var value = sortedIndex.GetValue(index2);
				return DoCompare(index1, value);
			}
		}

		#endregion
	}
}