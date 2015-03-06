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

using Deveel.Data.DbSystem;
using Deveel.Data.Sql;

namespace Deveel.Data.Index {
	public abstract class ColumnIndex : IDisposable {
		private static readonly BlockIndex<int> EmptyList;
		private static readonly BlockIndex<int> OneList;

		protected ColumnIndex(ITable table, int columnOffset) {
			ColumnOffset = columnOffset;
			Table = table;
		}

		~ColumnIndex() {
			Dispose(false);
		}

		static ColumnIndex() {
			EmptyList = new BlockIndex<int>();
			EmptyList.IsReadOnly = true;
			OneList = new BlockIndex<int>();
			OneList.Add(0);
			OneList.IsReadOnly = true;
		}

		public ITable Table { get; private set; }

		public int ColumnOffset { get; private set; }

		public bool IsReadOnly { get; set; }

		public abstract string Name { get; }

		protected DataObject GetValue(long row) {
			return Table.GetValue(row, ColumnOffset);
		}

		public abstract ColumnIndex Copy(ITable table, bool readOnly);

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
		}

		public abstract void Insert(int rowNumber);

		public abstract void Remove(int rowNumber);

		public IIndex<int> Order(IEnumerable<int> rows) {
			var rowSet = rows.ToList();

			// The length of the set to order
			int rowSetLength = rowSet.Count;

			// Trivial cases where sorting is not required:
			// NOTE: We use readOnly objects to save some memory.
			if (rowSetLength == 0)
				return EmptyList;
			if (rowSetLength == 1)
				return OneList;

			// This will be 'row set' sorted by its entry lookup.  This must only
			// contain indices to rowSet entries.
			var newSet = new BlockIndex<int>();

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
				var comparer = new SubsetIndexComparer(subsetList.ToArray());

				// Fill new_set with the set { 0, 1, 2, .... , row_set_length }
				for (int i = 0; i < rowSetLength; ++i) {
					var cell = subsetList[i];
					newSet.InsertSort(cell, i, comparer);
				}

			} else {
				// This is the no additional heap use method to sorting the sub-set.

				// The comparator we use to sort
				var comparer = new IndexComparer(this, rowSet);

				// Fill new_set with the set { 0, 1, 2, .... , row_set_length }
				for (int i = 0; i < rowSetLength; ++i) {
					var cell = GetValue(rowSet[i]);
					newSet.InsertSort(cell, i, comparer);
				}
			}

			return newSet;
		}

		public IEnumerable<int> SelectRange(IndexRange range) {
			return SelectRange(new[] {range});
		}

		public abstract IEnumerable<int> SelectRange(IndexRange[] ranges);

		public virtual IEnumerable<int> SelectAll() {
			return SelectRange(new IndexRange(
					 RangeFieldOffset.FirstValue, IndexRange.FirstInSet,
					 RangeFieldOffset.LastValue, IndexRange.LastInSet));
		}

		public virtual IEnumerable<int> SelectFirst() {
			// NOTE: This will find NULL at start which is probably wrong.  The
			//   first value should be the first non null value.
			return SelectRange(new IndexRange(
					 RangeFieldOffset.FirstValue, IndexRange.FirstInSet,
					 RangeFieldOffset.LastValue, IndexRange.FirstInSet));
		}

		public IEnumerable<int> SelectNotFirst() {
			// NOTE: This will find NULL at start which is probably wrong.  The
			//   first value should be the first non null value.
			return SelectRange(new IndexRange(
					 RangeFieldOffset.AfterLastValue, IndexRange.FirstInSet,
					 RangeFieldOffset.LastValue, IndexRange.LastInSet));
		}

		public IEnumerable<int> SelectLast() {
			return SelectRange(new IndexRange(
					 RangeFieldOffset.FirstValue, IndexRange.LastInSet,
					 RangeFieldOffset.LastValue, IndexRange.LastInSet));
		}

		public IEnumerable<int> SelectNotLast() {
			return SelectRange(new IndexRange(
					 RangeFieldOffset.FirstValue, IndexRange.FirstInSet,
					 RangeFieldOffset.BeforeFirstValue, IndexRange.LastInSet));
		}

		///<summary>
		/// Selects all values in the column that are not null.
		///</summary>
		///<returns></returns>
		public IEnumerable<int> SelectAllNonNull() {
			return SelectRange(new IndexRange(
						 RangeFieldOffset.AfterLastValue, DataObject.Null(),
						 RangeFieldOffset.LastValue, IndexRange.LastInSet));
		}

		public IEnumerable<int> SelectEqual(DataObject ob) {
			if (ob.IsNull)
				return new List<int>(0);

			return SelectRange(new IndexRange(
								 RangeFieldOffset.FirstValue, ob,
								 RangeFieldOffset.LastValue, ob));
		}

		public IEnumerable<int> SelectNotEqual(DataObject ob) {
			if (ob.IsNull) {
				return new List<int>(0);
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

		public IEnumerable<int> SelectGreater(DataObject ob) {
			if (ob.IsNull)
				return new List<int>(0);

			return SelectRange(new IndexRange(
					   RangeFieldOffset.AfterLastValue, ob,
					   RangeFieldOffset.LastValue, IndexRange.LastInSet));
		}

		public IEnumerable<int> SelectLess(DataObject ob) {
			if (ob.IsNull)
				return new List<int>(0);

			return SelectRange(new IndexRange(
					   RangeFieldOffset.AfterLastValue, DataObject.Null(),
					   RangeFieldOffset.BeforeFirstValue, ob));
		}

		public IEnumerable<int> SelectGreaterOrEqual(DataObject ob) {
			if (ob.IsNull)
				return new List<int>(0);

			return SelectRange(new IndexRange(
					   RangeFieldOffset.FirstValue, ob,
					   RangeFieldOffset.LastValue, IndexRange.LastInSet));
		}

		public IEnumerable<int> SelectLessOrEqual(DataObject ob) {
			if (ob.IsNull)
				return new List<int>(0);

			return SelectRange(new IndexRange(
					   RangeFieldOffset.AfterLastValue, DataObject.Null(),
					   RangeFieldOffset.LastValue, ob));
		}

		public IEnumerable<int> SelectBetween(DataObject ob1, DataObject ob2) {
			if (ob1.IsNull || ob2.IsNull)
				return new List<int>(0);

			return SelectRange(new IndexRange(
					   RangeFieldOffset.FirstValue, ob1,
					   RangeFieldOffset.BeforeFirstValue, ob2));
		}

		public virtual ColumnIndex GetSubset(ITable subsetTable, int subsetColumn) {
			if (subsetTable == null)
				throw new ArgumentNullException("subsetTable");

			if (!(subsetTable is IDbTable))
				throw new NotSupportedException("The type of table is not supported for this feature.");

			var dbTable = (IDbTable) subsetTable;

			// Resolve table rows in this table scheme domain.
			List<int> rowSet = new List<int>(subsetTable.RowCount);
			var e = subsetTable.GetEnumerator();
			while (e.MoveNext()) {
				rowSet.Add(e.Current.RowId.RowNumber);
			}

			var rows = dbTable.ResolveRows(subsetColumn, rowSet, Table);

			// Generates an IIndex which contains indices into 'rowSet' in
			// sorted order.
			var newSet = Order(rows);

			// Our 'new_set' should be the same size as 'rowSet'
			if (newSet.Count != rowSet.Count) {
				throw new Exception("Internal sort error in finding sub-set.");
			}

			return CreateSubset(subsetTable, subsetColumn, newSet);
		}

		protected virtual ColumnIndex CreateSubset(ITable table, int column, IEnumerable<int> rows) {
			var index = new InsertSearchIndex(table, column, rows);
			index.RecordUid = false;
			return index;
		}

		#region IndexComparer

		private class IndexComparer : IIndexComparer<int> {
			private readonly ColumnIndex scheme;
			private readonly IEnumerable<int> rowSet;

			public IndexComparer(ColumnIndex scheme, IEnumerable<int> rowSet) {
				this.scheme = scheme;
				this.rowSet = rowSet;
			}

			public int CompareValue(int index, DataObject val) {
				var cell = scheme.GetValue(rowSet.ElementAt((int)index));
				return cell.CompareTo(val);
			}

			public int Compare(int index1, int index2) {
				throw new NotSupportedException("Shouldn't be called!");
			}
		}

		#endregion

		#region SubsetIndexComparer

		private class SubsetIndexComparer : IIndexComparer<int> {
			private readonly DataObject[] subsetList;

			public SubsetIndexComparer(DataObject[] subsetList) {
				this.subsetList = subsetList;
			}

			public int CompareValue(int index, DataObject val) {
				var cell = subsetList[index];
				return cell.CompareTo(val);
			}

			public int Compare(int index1, int index2) {
				throw new NotSupportedException("Shouldn't be called!");
			}
		}

		#endregion
	}
}