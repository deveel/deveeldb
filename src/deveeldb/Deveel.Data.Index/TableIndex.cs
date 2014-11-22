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
	public abstract class TableIndex : IDisposable {
		private static readonly BlockIndex<long> EmptyList;
		private static readonly BlockIndex<long> OneList;

		protected TableIndex(ITable table, int columnOffset) {
			ColumnOffset = columnOffset;
			Table = table;
		}

		~TableIndex() {
			Dispose(false);
		}

		static TableIndex() {
			EmptyList = new BlockIndex<long>();
			EmptyList.IsReadOnly = true;
			OneList = new BlockIndex<long>();
			OneList.Add(0);
			OneList.IsReadOnly = true;
		}

		public ITable Table { get; private set; }

		public int ColumnOffset { get; private set; }

		public bool IsReadOnly { get; set; }

		protected DataObject GetValue(long row) {
			return Table.GetValue(row, ColumnOffset);
		}

		public abstract TableIndex Copy(ITable table, bool readOnly);

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
		}

		public abstract void Insert(long rowNumber);

		public abstract void Remove(long rowNumber);

		public IIndex<long> Order(IIndex<long> rowSet) {
			// The length of the set to order
			int rowSetLength = rowSet.Count();

			// Trivial cases where sorting is not required:
			// NOTE: We use readOnly objects to save some memory.
			if (rowSetLength == 0)
				return EmptyList;
			if (rowSetLength == 1)
				return OneList;

			// This will be 'row set' sorted by its entry lookup.  This must only
			// contain indices to rowSet entries.
			var newSet = new BlockIndex<long>();

			if (rowSetLength <= 250000) {
				// If the subset is less than or equal to 250,000 elements, we generate
				// an array in memory that contains all values in the set and we sort
				// it.  This requires use of memory from the heap but is faster than
				// the no heap use method.
				var subsetList = new List<DataObject>(rowSetLength);
				foreach (long row in rowSet) {
					subsetList.Add(GetValue(row));
				}

				// The comparator we use to sort
				var comparator = new SubsetIndexComparer(subsetList.ToArray());

				// Fill new_set with the set { 0, 1, 2, .... , row_set_length }
				for (int i = 0; i < rowSetLength; ++i) {
					var cell = subsetList[i];
					newSet.InsertSort(cell, i, comparator);
				}

			} else {
				// This is the no additional heap use method to sorting the sub-set.

				// The comparator we use to sort
				var comparator = new SchemeIndexComparer(this, rowSet);

				// Fill new_set with the set { 0, 1, 2, .... , row_set_length }
				for (int i = 0; i < rowSetLength; ++i) {
					var cell = GetValue(rowSet[i]);
					newSet.InsertSort(cell, i, comparator);
				}
			}

			return newSet;
		}

		public IEnumerable<long> SelectRange(IndexRange range) {
			return SelectRange(new[] {range});
		}

		public abstract IEnumerable<long> SelectRange(IndexRange[] ranges);

		public virtual IEnumerable<long> SelectAll() {
			return SelectRange(new IndexRange(
					 RangeFieldOffset.FirstValue, IndexRange.FirstInSet,
					 RangeFieldOffset.LastValue, IndexRange.LastInSet));
		}

		public virtual IEnumerable<long> SelectFirst() {
			// NOTE: This will find NULL at start which is probably wrong.  The
			//   first value should be the first non null value.
			return SelectRange(new IndexRange(
					 RangeFieldOffset.FirstValue, IndexRange.FirstInSet,
					 RangeFieldOffset.LastValue, IndexRange.FirstInSet));
		}

		public IEnumerable<long> SelectNotFirst() {
			// NOTE: This will find NULL at start which is probably wrong.  The
			//   first value should be the first non null value.
			return SelectRange(new IndexRange(
					 RangeFieldOffset.AfterLastValue, IndexRange.FirstInSet,
					 RangeFieldOffset.LastValue, IndexRange.LastInSet));
		}

		public IEnumerable<long> SelectLast() {
			return SelectRange(new IndexRange(
					 RangeFieldOffset.FirstValue, IndexRange.LastInSet,
					 RangeFieldOffset.LastValue, IndexRange.LastInSet));
		}

		public IEnumerable<long> SelectNotLast() {
			return SelectRange(new IndexRange(
					 RangeFieldOffset.FirstValue, IndexRange.FirstInSet,
					 RangeFieldOffset.BeforeFirstValue, IndexRange.LastInSet));
		}

		///<summary>
		/// Selects all values in the column that are not null.
		///</summary>
		///<returns></returns>
		public IEnumerable<long> SelectAllNonNull() {
			return SelectRange(new IndexRange(
						 RangeFieldOffset.AfterLastValue, DataObject.Null(),
						 RangeFieldOffset.LastValue, IndexRange.LastInSet));
		}

		public IEnumerable<long> SelectEqual(DataObject ob) {
			if (ob.IsNull)
				return new List<long>(0);

			return SelectRange(new IndexRange(
								 RangeFieldOffset.FirstValue, ob,
								 RangeFieldOffset.LastValue, ob));
		}

		public IEnumerable<long> SelectNotEqual(DataObject ob) {
			if (ob.IsNull) {
				return new List<long>(0);
			}
			return SelectRange(new IndexRange[]
			                   	{
			                   		new IndexRange(
			                   			RangeFieldOffset.AfterLastValue, DataObject.Null(),
			                   			RangeFieldOffset.BeforeFirstValue, ob)
			                   		, new IndexRange(
			                   		  	RangeFieldOffset.AfterLastValue, ob,
			                   		  	RangeFieldOffset.LastValue, IndexRange.LastInSet)
			                   	});
		}

		public IEnumerable<long> SelectGreater(DataObject ob) {
			if (ob.IsNull)
				return new List<long>(0);

			return SelectRange(new IndexRange(
					   RangeFieldOffset.AfterLastValue, ob,
					   RangeFieldOffset.LastValue, IndexRange.LastInSet));
		}

		public IEnumerable<long> SelectLess(DataObject ob) {
			if (ob.IsNull)
				return new List<long>(0);

			return SelectRange(new IndexRange(
					   RangeFieldOffset.AfterLastValue, DataObject.Null(),
					   RangeFieldOffset.BeforeFirstValue, ob));
		}

		public IEnumerable<long> SelectGreaterOrEqual(DataObject ob) {
			if (ob.IsNull)
				return new List<long>(0);

			return SelectRange(new IndexRange(
					   RangeFieldOffset.FirstValue, ob,
					   RangeFieldOffset.LastValue, IndexRange.LastInSet));
		}

		public IEnumerable<long> SelectLessOrEqual(DataObject ob) {
			if (ob.IsNull)
				return new List<long>(0);

			return SelectRange(new IndexRange(
					   RangeFieldOffset.AfterLastValue, DataObject.Null(),
					   RangeFieldOffset.LastValue, ob));
		}

		public IEnumerable<long> SelectBetween(DataObject ob1, DataObject ob2) {
			if (ob1.IsNull || ob2.IsNull)
				return new List<long>(0);

			return SelectRange(new IndexRange(
					   RangeFieldOffset.FirstValue, ob1,
					   RangeFieldOffset.BeforeFirstValue, ob2));
		}

		#region SchemeIndexComparer

		private class SchemeIndexComparer : IIndexComparer<long> {
			private readonly TableIndex scheme;
			private readonly IEnumerable<long> rowSet;

			public SchemeIndexComparer(TableIndex scheme, IEnumerable<long> rowSet) {
				this.scheme = scheme;
				this.rowSet = rowSet;
			}

			public int CompareValue(long index, DataObject val) {
				var cell = scheme.GetValue(rowSet.ElementAt((int)index));
				return cell.CompareTo(val);
			}

			public int Compare(long index1, long index2) {
				throw new NotSupportedException("Shouldn't be called!");
			}
		}

		#endregion

		#region SubsetIndexComparer

		private class SubsetIndexComparer : IIndexComparer<long> {
			private readonly DataObject[] subsetList;

			public SubsetIndexComparer(DataObject[] subsetList) {
				this.subsetList = subsetList;
			}

			public int CompareValue(long index, DataObject val) {
				var cell = subsetList[index];
				return cell.CompareTo(val);
			}

			public int Compare(long index1, long index2) {
				throw new NotSupportedException("Shouldn't be called!");
			}
		}

		#endregion
	}
}