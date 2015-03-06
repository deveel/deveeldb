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
//

using System;
using System.Collections.Generic;

using Deveel.Data.Sql;

namespace Deveel.Data.Index {
	public sealed class InsertSearchIndex : CollatedSearchIndex {
		private IIndex<int> list;

		private bool recordUid;
		private IIndexComparer<int> comparer;

		private readonly int readOnlyCount;

		public InsertSearchIndex(ITable table, int columnOffset) 
			: base(table, columnOffset) {
			list = new BlockIndex<int>();

			// The internal comparator that enables us to sort and lookup on the data
			// in this column.
			SetupComparer();
		}

		public InsertSearchIndex(ITable table, int columnOffset, IEnumerable<int> list)
			: this(table, columnOffset) {
			if (list != null) {
				foreach (var i in list) {
					this.list.Add(i);
				}
			}
		}

		public InsertSearchIndex(ITable table, int columnOffset, IIndex<int> list)
			: this(table, columnOffset) {
			this.list = list;
		}

		private InsertSearchIndex(ITable table, InsertSearchIndex source, bool readOnly)
			: base(table, source.ColumnOffset) {
			IsReadOnly = readOnly;

			if (readOnly) {
				list = source.list;
				readOnlyCount = list.Count;
			} else {
				list = new BlockIndex<int>(source.list);
			}

			// Do we generate lookup caches?
			recordUid = source.recordUid;

			// The internal comparator that enables us to sort and lookup on the data
			// in this column.
			SetupComparer();
		}

		protected override int Count {
			get { return list.Count; }
		}

		public override string Name {
			get { return DefaultIndexNames.InsertSearch; }
		}

		protected override DataObject First {
			get { return GetValue(list[0]); }
		}

		protected override DataObject Last {
			get { return GetValue(list[Count - 1]); }
		}

		internal bool RecordUid { get; set; }

		private void SetupComparer() {
			comparer = new IndexComparerImpl(this);
		}

		public override void Insert(int rowNumber) {
			if (IsReadOnly)
				throw new ApplicationException("Tried to change an readOnly scheme.");

			var cell = GetValue(rowNumber);
			list.InsertSort(cell, rowNumber, comparer);
		}

		public override void Remove(int rowNumber) {
			if (IsReadOnly)
				throw new ApplicationException("Tried to change an readOnly scheme.");

			var cell = GetValue(rowNumber);
			int removed = list.RemoveSort(cell, rowNumber, comparer);

			if (removed != rowNumber)
				throw new InvalidOperationException(String.Format("The row removes ({0}) is different than the one requested to be removed ({1})", removed, rowNumber));
		}

		public override ColumnIndex Copy(ITable table, bool readOnly) {
			// ASSERTION: If readOnly, check the size of the current set is equal to
			//   when the scheme was created.
			if (IsReadOnly && readOnlyCount != list.Count)
				throw new ApplicationException("Assert failed: read-only size is different from when created.");

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
			if (disposing) {
				list = null;
				comparer = null;
			}
		}

		#region IndexComparerImpl

		private class IndexComparerImpl : IIndexComparer<int> {
			private readonly InsertSearchIndex scheme;

			public IndexComparerImpl(InsertSearchIndex scheme) {
				this.scheme = scheme;
			}

			private int InternalCompare(int index, DataObject value) {
				var cell = scheme.GetValue(index);
				return cell.CompareTo(value);
			}

			public int CompareValue(int index, DataObject val) {
				return InternalCompare(index, val);
			}

			public int Compare(int index1, int index2) {
				var cell = scheme.GetValue(index2);
				return InternalCompare(index1, cell);
			}
		}

		#endregion
	}
}
