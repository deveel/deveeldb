// 
//  Copyright 2010  Deveel
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

using Deveel.Data.Util;

namespace Deveel.Data {
	/// <summary>
	/// This is a <see cref="SelectableScheme"/> similar in some ways 
	/// to the binary tree.
	/// </summary>
	/// <remarks>
	/// When a new row is added, it is inserted into a sorted list of rows.  
	/// We can then use this list to select out the sorted list of elements.
	/// <para>
	/// This requires less memory than the BinaryTree, however it is not as fast.
	/// Even though, it should still perform fairly well on medium size data sets.
	/// On large size data sets, insert and remove performance may suffer.
	/// </para>
	/// <para>
	/// This object retains knowledge of all set elements unlike BlindSearch which 
	/// has no memory overhead.
	/// </para>
	/// <para>
	/// Performance should be very comparable to BinaryTree for sets that aren't 
	/// altered much.
	/// </para>
	/// </remarks>
	public sealed class InsertSearch : CollatedBaseSearch {
		/// <summary>
		/// The sorted list of rows in this set.
		/// </summary>
		/// <remarks>
		/// This is sorted from min to max (not sorted by row number - sorted 
		/// by entity row value).
		/// </remarks>
		private IIndex list;

		/// <summary>
		/// If this is true, then this <see cref="SelectableScheme"/> records additional 
		/// rid information that can be used to very quickly identify whether a value is 
		/// greater, equal or less.
		/// </summary>
		private bool recordUid;

		/// <summary>
		/// The <see cref="IIndexComparer"/> that we use to refer elements in the set to 
		/// actual data objects.
		/// </summary>
		private IIndexComparer comparer;


		/// <summary>
		/// If this is readOnly, this stores the number of entries in <see cref="list"/> 
		/// when this object was made.
		/// </summary>
		private readonly int debugReadOnlySetSize;

		public InsertSearch(ITableDataSource table, int column)
			: base(table, column) {
			list = new BlockIndex();

			// The internal comparator that enables us to sort and lookup on the data
			// in this column.
			SetupComparer();
		}


		/// <summary>
		/// Constructor sets the scheme with a pre-sorted list.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="column"></param>
		/// <param name="list">A sorted list, with a low to high direction order, that is used to
		/// set the scheme. This should not be used again after it is passed to this constructor.</param>
		public InsertSearch(ITableDataSource table, int column, IEnumerable<int> list)
			: this(table, column) {
			foreach (int i in list) {
				this.list.Add(i);
			}
		}

		/// <summary>
		/// Constructor sets the scheme with a pre-sorted list.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="column"></param>
		/// <param name="list">A sorted list, with a low to high direction order, that is used to
		/// set the scheme. This should not be used again after it is passed to this constructor.</param>
		internal InsertSearch(ITableDataSource table, int column, IIndex list)
			: this(table, column) {
			this.list = list;
		}

		/// <summary>
		/// Constructs this as a copy of the given, either mutable or readOnly copy.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="from"></param>
		/// <param name="readOnly"></param>
		private InsertSearch(ITableDataSource table, InsertSearch from, bool readOnly)
			: base(table, from.Column) {
			IsReadOnly = readOnly;

			if (readOnly) {
				// Immutable is a shallow copy
				list = from.list;
				debugReadOnlySetSize = list.Count;
			} else {
				list = new BlockIndex(from.list);
			}

			// Do we generate lookup caches?
			recordUid = from.recordUid;

			// The internal comparator that enables us to sort and lookup on the data
			// in this column.
			SetupComparer();
		}

		/// <summary>
		/// Sets the internal comparator that enables us to sort and lookup on the
		/// data in this column.
		/// </summary>
		private void SetupComparer() {
			comparer = new IndexComparerImpl(this);
		}

		/// <summary>
		/// Inserts a row into the list.
		/// </summary>
		/// <param name="row"></param>
		/// <remarks>
		/// This will always be thread safe, table changes cause a Write Lock which 
		/// prevents reads while we are writing to the table.
		/// </remarks>
		public override void Insert(int row) {
			if (IsReadOnly)
				throw new ApplicationException("Tried to change an readOnly scheme.");

			TObject cell = GetCellContents(row);
			list.InsertSort(cell, row, comparer);
		}

		/// <summary>
		/// Removes a row from the list.
		/// </summary>
		/// <param name="row"></param>
		/// <remarks>
		/// This will always be thread safe, table changes cause a Write Lock which 
		/// prevents reads while we are writing to the table.
		/// </remarks>
		public override void Remove(int row) {
			if (IsReadOnly)
				throw new ApplicationException("Tried to change an readOnly scheme.");

			TObject cell = GetCellContents(row);
			int removed = list.RemoveSort(cell, row, comparer);

			if (removed != row) {
				throw new ApplicationException("Removed value different than row asked to remove.  " +
								"To remove: " + row + "  Removed: " + removed);
			}
		}

		/// <summary>
		/// Returns an exact copy of this scheme including any optimization
		/// information.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="immutable"></param>
		/// <remarks>
		/// The copied scheme is identical to the original but does not share any 
		/// parts. Modifying any part of the copied scheme will have no effect on 
		/// the original and vice versa.
		/// </remarks>
		/// <returns></returns>
		public override SelectableScheme Copy(ITableDataSource table, bool immutable) {
			// ASSERTION: If readOnly, check the size of the current set is equal to
			//   when the scheme was created.
			if (IsReadOnly) {
				if (debugReadOnlySetSize != list.Count) {
					throw new ApplicationException("Assert failed: " +
									"Immutable set size is different from when created.");
				}
			}

			// We must create a new InsertSearch object and copy all the state
			// information from this object to the new one.
			return new InsertSearch(table, this, immutable);
		}

		/// <summary>
		/// Disposes this scheme.
		/// </summary>
		public override void Dispose() {
			// Close and invalidate.
			list = null;
			comparer = null;
		}

		// ---------- Implemented/Overwritten from CollatedBaseSearch ----------

		protected override int SearchFirst(TObject val) {
			return list.SearchFirst(val, comparer);
		}

		protected override int SearchLast(TObject val) {
			return list.SearchLast(val, comparer);
		}

		protected override int SetSize {
			get { return list.Count; }
		}

		protected override TObject FirstInCollationOrder {
			get { return GetCellContents(list[0]); }
		}

		protected override TObject LastInCollationOrder {
			get { return GetCellContents(list[SetSize - 1]); }
		}

		/// <summary>
		/// If this is true, then this <see cref="SelectableScheme"/> records additional 
		/// rid information that can be used to very quickly identify whether a value is 
		/// greater, equal or less.
		/// </summary>
		internal bool RecordUid {
			get { return recordUid; }
			set { recordUid = value; }
		}

		protected override IList<int> AddRangeToSet(int start, int end, IList<int> list) {
			if (list == null) {
				list = new List<int>((end - start) + 2);
			}
			IIndexEnumerator i = this.list.GetEnumerator(start, end);
			while (i.MoveNext()) {
				list.Add(i.Current);
			}
			return list;
		}

		public override IList<int> SelectAll() {
			return ListUtil.ToList(list);
		}

		private class IndexComparerImpl : IIndexComparer {
			private readonly InsertSearch scheme;

			public IndexComparerImpl(InsertSearch scheme) {
				this.scheme = scheme;
			}

			private int InternalCompare(int index, TObject value) {
				TObject cell = scheme.GetCellContents(index);
				return cell.CompareTo(value);
			}

			public int Compare(int index, object val) {
				return InternalCompare(index, (TObject)val);
			}

			public int Compare(int index1, int index2) {
				TObject cell = scheme.GetCellContents(index2);
				return InternalCompare(index1, cell);
			}
		}
	}
}