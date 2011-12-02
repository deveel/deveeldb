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
using System.Text;

using Deveel.Data.Collections;
using Deveel.Diagnostics;
using Deveel.Data.Util;

namespace Deveel.Data {
	/// <summary>
	/// Represents a base class for a mechanism to select ranges from a 
	/// given set.
	/// </summary>
	/// <remarks>
	/// Such schemes could include BinaryTree, Hashtable or just a blind 
	/// search.
	/// <para>
	/// A given element in the set is specified through a 'row' integer whose
	/// contents can be obtained through the 'table.GetCellContents(column, row)'.
	/// Every scheme is given a table and column number that the set refers to.
	/// While a given set element is refered to as a 'row', the integer is really
	/// only a pointer into the set list which can be de-referenced with a call to
	/// <see cref="ITableDataSource.GetCellContents"/>.  Better performance schemes 
	/// will keep such calls to a minimum.
	/// </para>
	/// <para>
	/// A scheme may choose to retain knowledge about a given element when it is
	/// added or removed from the set, such as a BinaryTree that catalogs all
	/// elements with respect to each other.
	/// </para>
	/// </remarks>
	public abstract class SelectableScheme {
		private static readonly BlockIntegerList EMPTY_LIST;
		private static readonly BlockIntegerList ONE_LIST;

		static SelectableScheme() {
			EMPTY_LIST = new BlockIntegerList();
			EMPTY_LIST.SetImmutable();
			ONE_LIST = new BlockIntegerList();
			ONE_LIST.Add(0);
			ONE_LIST.SetImmutable();
		}

		/// <summary>
		/// The table data source with the column this scheme indexes.
		/// </summary>
		private readonly ITableDataSource table;

		/// <summary>
		/// The column number in the tree this tree helps.
		/// </summary>
		private readonly int column;

		/// <summary>
		/// Set to true if this scheme is immutable (can't be changed).
		/// </summary>
		private bool immutable;

		protected SelectableScheme(ITableDataSource table, int column) {
			this.table = table;
			this.column = column;
		}

		/// <summary>
		/// Returns the Table.
		/// </summary>
		protected ITableDataSource Table {
			get { return table; }
		}

		/// <summary>
		/// Returns the global transaction system.
		/// </summary>
		protected TransactionSystem System {
			get { return table.System; }
		}

		/// <summary>
		/// Returns the <see cref="IDebugLogger"/> object to log debug 
		/// messages to.
		/// </summary>
		/*
		TODO:
		protected IDebugLogger Debug {
			get { return System.Debug; }
		}
		*/

		/// <summary>
		/// Returns the column this scheme is indexing in the table.
		/// </summary>
		protected int Column {
			get { return column; }
		}

		/// <summary>
		/// Obtains the given cell in the row from the table.
		/// </summary>
		/// <param name="row"></param>
		/// <returns></returns>
		protected TObject GetCellContents(int row) {
			return table.GetCellContents(column, row);
		}

		/// <summary>
		/// Sets this scheme to immutable.
		/// </summary>
		public void SetImmutable() {
			immutable = true;
		}

		/// <summary>
		/// Returns true if this scheme is immutable.
		/// </summary>
		public bool IsImmutable {
			get { return immutable; }
		}

		/// <inheritdoc/>
		public override string ToString() {
			// Name of the table
			String table_name;
			if (table is DefaultDataTable) {
				table_name = ((DefaultDataTable)table).TableName.ToString();
			} else {
				table_name = "VirtualTable";
			}

			StringBuilder buf = new StringBuilder();
			buf.Append("[ SelectableScheme ");
			buf.Append(base.ToString());
			buf.Append(" for table: ");
			buf.Append(table_name);
			buf.Append("]");

			return buf.ToString();
		}

		/// <summary>
		/// Returns an exact copy of this scheme including any optimization
		/// information.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="immutable"></param>
		/// <remarks>
		/// The copied scheme is identical to the original but does not share 
		/// any parts. Modifying any part of the copied scheme will have no
		/// effect on the original and vice versa.
		/// <para>
		/// The newly copied scheme can be given a new table source. If
		/// 'immutable' is true, then the resultant scheme is an immutable 
		/// version of the parent. An immutable version may share information 
		/// with the copied version so can not be changed.
		/// </para>
		/// <para>
		/// <b>Note</b> Even if the scheme maintains no state you should still 
		/// be careful to ensure a fresh <see cref="SelectableScheme"/> object 
		/// is returned here.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public abstract SelectableScheme Copy(ITableDataSource table, bool immutable);

		/// <summary>
		/// Dispose and invalidate this scheme.
		/// </summary>
		public abstract void Dispose();


		/**
		 * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
		 * Abstract methods for selection of rows, and maintenance of rows
		 * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
		 */

		/// <summary>
		/// Inserts the given element into the set.
		/// </summary>
		/// <param name="row"></param>
		/// <remarks>
		/// This is called just after a row has been initially added to a table.
		/// </remarks>
		internal abstract void Insert(int row);

		/// <summary>
		/// Removes the given element from the set.
		/// </summary>
		/// <param name="row"></param>
		/// <remarks>
		/// This is called just before the row is removed from the table.
		/// </remarks>
		internal abstract void Remove(int row);

		/// <summary>
		/// Sorts the given row set in the order of the scheme.
		/// </summary>
		/// <param name="row_set"></param>
		/// <remarks>
		/// The values in <paramref name="row_set"/> must be references to rows in 
		/// the domain of the table this scheme represents.
		/// <para>
		/// The returned set must be stable, meaning if values are equal they 
		/// keep the same ordering.
		/// </para>
		/// <para>
		/// Note that the default implementation of this method can often be 
		/// optimized. For example, <see cref="InsertSearch"/> uses a secondary 
		/// RID list to sort items if the given list is over a certain size.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns a <see cref="BlockIntegerList"/> that represents the given 
		/// <paramref name="row_set"/> sorted in the order of this scheme.
		/// </returns>
		public IIntegerList InternalOrderIndexSet(IntegerVector row_set) {
			// The length of the set to order
			int row_set_length = row_set.Count;

			// Trivial cases where sorting is not required:
			// NOTE: We use immutable objects to save some memory.
			if (row_set_length == 0) {
				return EMPTY_LIST;
			} else if (row_set_length == 1) {
				return ONE_LIST;
			}

			// This will be 'row_set' sorted by its entry lookup.  This must only
			// contain indices to row_set entries.
			BlockIntegerList new_set = new BlockIntegerList();

			if (row_set_length <= 250000) {
				// If the subset is less than or equal to 250,000 elements, we generate
				// an array in memory that contains all values in the set and we sort
				// it.  This requires use of memory from the heap but is faster than
				// the no heap use method.
				TObject[] subset_list = new TObject[row_set_length];
				for (int i = 0; i < row_set_length; ++i) {
					subset_list[i] = GetCellContents(row_set[i]);
				}

				// The comparator we use to sort
				IIndexComparer comparator = new IndexComparatorImpl1(subset_list);

				// Fill new_set with the set { 0, 1, 2, .... , row_set_length }
				for (int i = 0; i < row_set_length; ++i) {
					TObject cell = subset_list[i];
					new_set.InsertSort(cell, i, comparator);
				}

			} else {
				// This is the no additional heap use method to sorting the sub-set.

				// The comparator we use to sort
				IIndexComparer comparator = new IndexComparatorImpl2(this, row_set);

				// Fill new_set with the set { 0, 1, 2, .... , row_set_length }
				for (int i = 0; i < row_set_length; ++i) {
					TObject cell = GetCellContents(row_set[i]);
					new_set.InsertSort(cell, i, comparator);
				}

			}

			return new_set;

		}

		private class IndexComparatorImpl1 : IIndexComparer {
			private readonly TObject[] subset_list;

			public IndexComparatorImpl1(TObject[] subsetList) {
				subset_list = subsetList;
			}

			public int Compare(int index, Object val) {
				TObject cell = subset_list[index];
				return cell.CompareTo((TObject)val);
			}
			public int Compare(int index1, int index2) {
				throw new NotSupportedException("Shouldn't be called!");
			}

			#region Implementation of IComparer

			public int Compare(object x, object y) {
				return Compare((int) x, y);
			}

			#endregion
		}

		private class IndexComparatorImpl2 : IIndexComparer {
			private readonly SelectableScheme scheme;
			private readonly IntegerVector row_set;

			public IndexComparatorImpl2(SelectableScheme scheme, IntegerVector row_set) {
				this.scheme = scheme;
				this.row_set = row_set;
			}

			public int Compare(int index, Object val) {
				TObject cell = scheme.GetCellContents(row_set[index]);
				return cell.CompareTo((TObject)val);
			}
			public int Compare(int index1, int index2) {
				throw new NotSupportedException("Shouldn't be called!");
			}

			#region Implementation of IComparer

			public int Compare(object x, object y) {
				return Compare((int) x, y);
			}

			#endregion
		}

		/// <summary>
		/// Asks the <see cref="SelectableScheme">scheme</see> for a 
		/// <see cref="SelectableScheme"/> object that describes a 
		/// sub-set of the set handled by this scheme.
		/// </summary>
		/// <param name="subset_table"></param>
		/// <param name="subset_column"></param>
		/// <remarks>
		/// Since a <see cref="Table">table</see> stores a subset of a given 
		/// <see cref="DataTable"/>, we pass this as the argument.  It returns 
		/// a new <see cref="SelectableScheme"/> that orders the rows in the 
		/// given columns order.
		/// The  <see cref="column"/> variable specifies the column index of 
		/// this column in the given table.
		/// </remarks>
		/// <returns></returns>
		public SelectableScheme GetSubsetScheme(Table subset_table, int subset_column) {
			// Resolve table rows in this table scheme domain.
			IntegerVector row_set = new IntegerVector(subset_table.RowCount);
			IRowEnumerator e = subset_table.GetRowEnumerator();
			while (e.MoveNext()) {
				row_set.AddInt(e.RowIndex);
			}
			subset_table.SetToRowTableDomain(subset_column, row_set, Table);

			// Generates an IntegerVector which contains indices into 'row_set' in
			// sorted order.
			IIntegerList new_set = InternalOrderIndexSet(row_set);

			// Our 'new_set' should be the same size as 'row_set'
			if (new_set.Count != row_set.Count) {
				throw new Exception("Internal sort error in finding sub-set.");
			}

			// Set up a new SelectableScheme with the sorted index set.
			// Move the sorted index set into the new scheme.
			InsertSearch scheme = new InsertSearch(subset_table, subset_column, new_set);
			// Don't let subset schemes create uid caches.
			scheme.RECORD_UID = false;
			return scheme;

		}

		/**
		 * These are the select operations that are the main purpose of the scheme.
		 * They retrieve the given information from the set.  Different schemes will
		 * have varying performance on different types of data sets.
		 * The select operations must *always* return a resultant row set that
		 * is sorted from lowest to highest.
		 */

		///<summary>
		///</summary>
		///<returns></returns>
		public virtual IntegerVector SelectAll() {
			return SelectRange(new SelectableRange(
					 SelectableRange.FIRST_VALUE, SelectableRange.FIRST_IN_SET,
					 SelectableRange.LAST_VALUE, SelectableRange.LAST_IN_SET));
		}

		///<summary>
		///</summary>
		///<returns></returns>
		public virtual IntegerVector SelectFirst() {
			// NOTE: This will find NULL at start which is probably wrong.  The
			//   first value should be the first non null value.
			return SelectRange(new SelectableRange(
					 SelectableRange.FIRST_VALUE, SelectableRange.FIRST_IN_SET,
					 SelectableRange.LAST_VALUE, SelectableRange.FIRST_IN_SET));
		}

		///<summary>
		///</summary>
		///<returns></returns>
		public IntegerVector SelectNotFirst() {
			// NOTE: This will find NULL at start which is probably wrong.  The
			//   first value should be the first non null value.
			return SelectRange(new SelectableRange(
					 SelectableRange.AFTER_LAST_VALUE, SelectableRange.FIRST_IN_SET,
					 SelectableRange.LAST_VALUE, SelectableRange.LAST_IN_SET));
		}

		///<summary>
		///</summary>
		///<returns></returns>
		public IntegerVector SelectLast() {
			return SelectRange(new SelectableRange(
					 SelectableRange.FIRST_VALUE, SelectableRange.LAST_IN_SET,
					 SelectableRange.LAST_VALUE, SelectableRange.LAST_IN_SET));
		}

		///<summary>
		///</summary>
		///<returns></returns>
		public IntegerVector SelectNotLast() {
			return SelectRange(new SelectableRange(
					 SelectableRange.FIRST_VALUE, SelectableRange.FIRST_IN_SET,
					 SelectableRange.BEFORE_FIRST_VALUE, SelectableRange.LAST_IN_SET));
		}

		///<summary>
		/// Selects all values in the column that are not null.
		///</summary>
		///<returns></returns>
		public IntegerVector SelectAllNonNull() {
			return SelectRange(new SelectableRange(
						 SelectableRange.AFTER_LAST_VALUE, TObject.Null,
						 SelectableRange.LAST_VALUE, SelectableRange.LAST_IN_SET));
		}

		///<summary>
		///</summary>
		///<param name="ob"></param>
		///<returns></returns>
		public IntegerVector SelectEqual(TObject ob) {
			if (ob.IsNull) {
				return new IntegerVector(0);
			}
			return SelectRange(new SelectableRange(
								 SelectableRange.FIRST_VALUE, ob,
								 SelectableRange.LAST_VALUE, ob));
		}

		///<summary>
		///</summary>
		///<param name="ob"></param>
		///<returns></returns>
		public IntegerVector SelectNotEqual(TObject ob) {
			if (ob.IsNull) {
				return new IntegerVector(0);
			}
			return SelectRange(new SelectableRange[]
			                   	{
			                   		new SelectableRange(
			                   			SelectableRange.AFTER_LAST_VALUE, TObject.Null,
			                   			SelectableRange.BEFORE_FIRST_VALUE, ob)
			                   		, new SelectableRange(
			                   		  	SelectableRange.AFTER_LAST_VALUE, ob,
			                   		  	SelectableRange.LAST_VALUE, SelectableRange.LAST_IN_SET)
			                   	});
		}

		///<summary>
		///</summary>
		///<param name="ob"></param>
		///<returns></returns>
		public IntegerVector SelectGreater(TObject ob) {
			if (ob.IsNull) {
				return new IntegerVector(0);
			}
			return SelectRange(new SelectableRange(
					   SelectableRange.AFTER_LAST_VALUE, ob,
					   SelectableRange.LAST_VALUE, SelectableRange.LAST_IN_SET));
		}

		///<summary>
		///</summary>
		///<param name="ob"></param>
		///<returns></returns>
		public IntegerVector SelectLess(TObject ob) {
			if (ob.IsNull) {
				return new IntegerVector(0);
			}
			return SelectRange(new SelectableRange(
					   SelectableRange.AFTER_LAST_VALUE, TObject.Null,
					   SelectableRange.BEFORE_FIRST_VALUE, ob));
		}

		///<summary>
		///</summary>
		///<param name="ob"></param>
		///<returns></returns>
		public IntegerVector SelectGreaterOrEqual(TObject ob) {
			if (ob.IsNull) {
				return new IntegerVector(0);
			}
			return SelectRange(new SelectableRange(
					   SelectableRange.FIRST_VALUE, ob,
					   SelectableRange.LAST_VALUE, SelectableRange.LAST_IN_SET));
		}

		///<summary>
		///</summary>
		///<param name="ob"></param>
		///<returns></returns>
		public IntegerVector SelectLessOrEqual(TObject ob) {
			if (ob.IsNull) {
				return new IntegerVector(0);
			}
			return SelectRange(new SelectableRange(
					   SelectableRange.AFTER_LAST_VALUE, TObject.Null,
					   SelectableRange.LAST_VALUE, ob));
		}

		// Inclusive of rows that are >= ob1 and < ob2
		// NOTE: This is not compatible with SQL BETWEEN predicate which is all
		//   rows that are >= ob1 and <= ob2
		///<summary>
		///</summary>
		///<param name="ob1"></param>
		///<param name="ob2"></param>
		///<returns></returns>
		public IntegerVector SelectBetween(TObject ob1, TObject ob2) {
			if (ob1.IsNull || ob2.IsNull) {
				return new IntegerVector(0);
			}
			return SelectRange(new SelectableRange(
					   SelectableRange.FIRST_VALUE, ob1,
					   SelectableRange.BEFORE_FIRST_VALUE, ob2));
		}

		/// <summary>
		/// Selects the given range of values from this index.
		/// </summary>
		/// <param name="range"></param>
		/// <remarks>
		/// The <see cref="SelectableRange"/> must contain a 
		/// <see cref="SelectableRange.Start"/> value that compares &lt;= to 
		/// the <see cref="SelectableRange.End"/> value.
		/// <para>
		/// This must guarentee that the returned set is sorted from lowest to
		/// highest value.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		internal abstract IntegerVector SelectRange(SelectableRange range);

		/// <summary>
		/// Selects a set of ranges from this index.
		/// </summary>
		/// <param name="ranges"></param>
		/// <remarks>
		/// The ranges must not overlap and each range must contain a 'start' 
		/// value that compares &lt;= to the 'end' value. Every range in the 
		/// array must represent a range that's lower than the preceeding range 
		/// (if it exists).
		/// <para>
		/// If the above rules are enforced (as they must be) then this method 
		/// will return a set that is sorted from lowest to highest value.
		/// </para>
		/// <para>
		/// This must guarentee that the returned set is sorted from lowest to
		/// highest value.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		internal abstract IntegerVector SelectRange(SelectableRange[] ranges);

	}
}