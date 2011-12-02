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
using System.IO;

using Deveel.Data.Collections;
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
		private IIntegerList set_list;

		/// <summary>
		/// If this is true, then this <see cref="SelectableScheme"/> records additional 
		/// rid information that can be used to very quickly identify whether a value is 
		/// greater, equal or less.
		/// </summary>
		internal bool RECORD_UID;

		/// <summary>
		/// The <see cref="IIndexComparer"/> that we use to refer elements in the set to 
		/// actual data objects.
		/// </summary>
		private IIndexComparer set_comparator;


		// ----- DEBUGGING -----

		/// <summary>
		/// If this is immutable, this stores the number of entries in <see cref="set_list"/> 
		/// when this object was made.
		/// </summary>
		private readonly int DEBUG_immutable_set_size;




		public InsertSearch(ITableDataSource table, int column)
			: base(table, column) {
			set_list = new BlockIntegerList();

			// The internal comparator that enables us to sort and lookup on the data
			// in this column.
			SetupComparer();
		}


		/// <summary>
		/// Constructor sets the scheme with a pre-sorted list.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="column"></param>
		/// <param name="vec">A sorted list, with a low to high direction order, that is used to
		/// set the scheme. This should not be used again after it is passed to this constructor.</param>
		public InsertSearch(ITableDataSource table, int column, IntegerVector vec)
			: this(table, column) {
			for (int i = 0; i < vec.Count; ++i) {
				set_list.Add(vec[i]);
			}

			// NOTE: This must be removed in final, this is a post condition check to
			//   make sure 'vec' is infact sorted
			//checkSchemeSorted();

		}

		/// <summary>
		/// Constructor sets the scheme with a pre-sorted list.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="column"></param>
		/// <param name="list">A sorted list, with a low to high direction order, that is used to
		/// set the scheme. This should not be used again after it is passed to this constructor.</param>
		internal InsertSearch(ITableDataSource table, int column, IIntegerList list)
			: this(table, column) {
			this.set_list = list;

			// NOTE: This must be removed in final, this is a post condition check to
			//   make sure 'vec' is infact sorted
			//checkSchemeSorted();

		}

		/// <summary>
		/// Constructs this as a copy of the given, either mutable or immutable copy.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="from"></param>
		/// <param name="immutable"></param>
		private InsertSearch(ITableDataSource table, InsertSearch from, bool immutable)
			: base(table, from.Column) {

			if (immutable) {
				SetImmutable();
			}

			if (immutable) {
				// Immutable is a shallow copy
				set_list = from.set_list;
				DEBUG_immutable_set_size = set_list.Count;
			} else {
				set_list = new BlockIntegerList(from.set_list);
			}

			// Do we generate lookup caches?
			RECORD_UID = from.RECORD_UID;

			// The internal comparator that enables us to sort and lookup on the data
			// in this column.
			SetupComparer();

		}

		/// <summary>
		/// Sets the internal comparator that enables us to sort and lookup on the
		/// data in this column.
		/// </summary>
		private void SetupComparer() {
			set_comparator = new IndexComparatorImpl(this);
		}


		private class IndexComparatorImpl : IIndexComparer {
			private readonly InsertSearch scheme;

			public IndexComparatorImpl(InsertSearch scheme) {
				this.scheme = scheme;
			}

			private int InternalCompare(int index, TObject cell2) {
				TObject cell1 = scheme.GetCellContents(index);
				return cell1.CompareTo(cell2);
			}

			public int Compare(int index, Object val) {
				return InternalCompare(index, (TObject)val);
			}
			public int Compare(int index1, int index2) {
				TObject cell = scheme.GetCellContents(index2);
				return InternalCompare(index1, cell);
			}

			#region Implementation of IComparer

			public int Compare(object x, object y) {
				return y is int ? Compare((int) x, (int) y) : Compare((int) x, y);
			}

			#endregion
		}


		/// <summary>
		/// Inserts a row into the list.
		/// </summary>
		/// <param name="row"></param>
		/// <remarks>
		/// This will always be thread safe, table changes cause a Write Lock which 
		/// prevents reads while we are writing to the table.
		/// </remarks>
		internal override void Insert(int row) {
			if (IsImmutable) {
				throw new ApplicationException("Tried to change an immutable scheme.");
			}

			TObject cell = GetCellContents(row);
			set_list.InsertSort(cell, row, set_comparator);

		}

		/// <summary>
		/// Removes a row from the list.
		/// </summary>
		/// <param name="row"></param>
		/// <remarks>
		/// This will always be thread safe, table changes cause a Write Lock which 
		/// prevents reads while we are writing to the table.
		/// </remarks>
		internal override void Remove(int row) {
			if (IsImmutable) {
				throw new ApplicationException("Tried to change an immutable scheme.");
			}

			TObject cell = GetCellContents(row);
			int removed = set_list.RemoveSort(cell, row, set_comparator);

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
			// ASSERTION: If immutable, check the size of the current set is equal to
			//   when the scheme was created.
			if (IsImmutable) {
				if (DEBUG_immutable_set_size != set_list.Count) {
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
			set_list = null;
			set_comparator = null;
		}

		// ---------- Implemented/Overwritten from CollatedBaseSearch ----------

		protected override int SearchFirst(TObject val) {
			return set_list.SearchFirst(val, set_comparator);
		}

		protected override int SearchLast(TObject val) {
			return set_list.SearchLast(val, set_comparator);
		}

		protected override int SetSize {
			get { return set_list.Count; }
		}

		protected override TObject FirstInCollationOrder {
			get { return GetCellContents(set_list[0]); }
		}

		protected override TObject LastInCollationOrder {
			get { return GetCellContents(set_list[SetSize - 1]); }
		}

		protected override IntegerVector AddRangeToSet(int start, int end,
											  IntegerVector ivec) {
			if (ivec == null) {
				ivec = new IntegerVector((end - start) + 2);
			}
			IIntegerIterator i = set_list.GetIterator(start, end);
			while (i.MoveNext()) {
				ivec.AddInt(i.Next);
			}
			return ivec;
		}

		public override IntegerVector SelectAll() {
			IntegerVector ivec = new IntegerVector(set_list);
			return ivec;
		}

	}
}