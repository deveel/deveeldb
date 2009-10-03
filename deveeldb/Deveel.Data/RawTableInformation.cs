// 
//  RawTableInformation.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

using Deveel.Data.Collections;

namespace Deveel.Data {
	/// <summary>
	/// This object represents the lowest level <see cref="DataTable"/> information 
	/// of a given <see cref="VirtualTable"/>.
	/// </summary>
	/// <remarks>
	/// Since it is possible to make any level of <see cref="VirtualTable"/>'s, it 
	/// is useful  to be able to resolve an <i>n leveled</i> <see cref="VirtualTable"/> 
	/// to a single level table.
	/// This object is used to collect information as the <see cref="JoinedTable.ResolveToRawTable(RawTableInformation)"/> 
	/// method is walking throught the <see cref="VirtualTable"/>'s ancestors.
	/// </remarks>
	sealed class RawTableInformation {
		/// <summary>
		/// A Vector containing a list of DataTables, and 'row index' IntegerVectors
		/// of the given rows in the table.
		/// </summary>
		private readonly ArrayList raw_info;

		internal RawTableInformation() {
			raw_info = new ArrayList();
		}

		/// <summary>
		/// Adds a new <see cref="DataTable"/> or <see cref="ReferenceTable"/>, 
		/// and <see cref="IntegerVector"/> row set into
		/// the object.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="row_set"></param>
		/// <remarks>
		/// We can not add <see cref="VirtualTable"/> objects into this object.
		/// </remarks>
		internal void Add(IRootTable table, IntegerVector row_set) {
			RawTableElement elem = new RawTableElement();
			elem.table = table;
			elem.row_set = row_set;
			raw_info.Add(elem);
		}

		/// <summary>
		/// Returns an array of <see cref="Table"/>of all the tables that have 
		/// been added.
		/// </summary>
		internal Table[] GetTables() {
			int size = raw_info.Count;
			Table[] list = new Table[size];
			for (int i = 0; i < size; ++i) {
				list[i] = (Table)((RawTableElement)raw_info[i]).table;
			}
			return list;
		}

		/// <summary>
		/// Returns an array of <see cref="IntegerVector"/> of the rows in the 
		/// table that have been added.
		/// </summary>
		internal IntegerVector[] GetRows() {
			int size = raw_info.Count;
			IntegerVector[] list = new IntegerVector[size];
			for (int i = 0; i < size; ++i) {
				list[i] = ((RawTableElement)raw_info[i]).row_set;
			}
			return list;
		}

		/// <summary>
		/// Returns an array of RawTableElement sorted into a consistant order.
		/// </summary>
		/// <returns></returns>
		private RawTableElement[] GetSortedElements() {
			RawTableElement[] list = new RawTableElement[raw_info.Count];
			raw_info.CopyTo(list);
			//TODO: SortUtil.QuickSort(list);
			Array.Sort(list);
			return list;
		}

		/// <summary>
		/// Finds the union of this information with the given information.
		/// </summary>
		/// <param name="info"></param>
		/// <remarks>
		/// It does the following:
		/// <list type="bullet">
		/// <item>Sorts the unioned tables into a consistant order.</item>
		/// <item>Merges each row in the tables row_set.</item>
		/// <item>Sorts the resultant merge.</item>
		/// <item>Makes a new set with the resultant merge minus any duplicates.</item>
		/// </list>
		/// </remarks>
		internal void Union(RawTableInformation info) {

			// Number of Table 'columns'

			int col_count = raw_info.Count;

			// Get the sorted RawTableElement[] from each raw table information object.

			RawTableElement[] merge1 = GetSortedElements();
			RawTableElement[] merge2 = info.GetSortedElements();

			// Validates that both tables being merges are of identical type.

			int size1 = -1;
			int size2 = -1;

			// First check number of tables in each merge is correct.

			if (merge1.Length != merge2.Length) {
				throw new ApplicationException("Incorrect format in table union");
			}

			// Check each table in the merge1 set has identical length row_sets

			for (int i = 0; i < merge1.Length; ++i) {
				if (size1 == -1) {
					size1 = merge1[i].row_set.Count;
				} else {
					if (size1 != merge1[i].row_set.Count) {
						throw new ApplicationException("Incorrect format in table union");
					}
				}
			}

			// Check each table in the merge2 set has identical length row_sets

			for (int i = 0; i < merge2.Length; ++i) {

				// Check the tables in merge2 are identical to the tables in merge1
				// (Checks the names match, and the validColumns filters are identical
				//  see DataTableBase.TypeEquals method).

				if (!merge2[i].table.TypeEquals(merge1[i].table)) {
					throw new ApplicationException("Incorrect format in table union");
				}

				if (size2 == -1) {
					size2 = merge2[i].row_set.Count;
				} else {
					if (size2 != merge2[i].row_set.Count) {
						throw new ApplicationException("Incorrect format in table union");
					}
				}
			}

			// If size1 or size2 are -1 then we have a corrupt table.  (It will be
			// 0 for an empty table).

			if (size1 == -1 || size2 == -1) {
				throw new ApplicationException("Incorrect format in table union");
			}

			// We don't need information in 'raw_info' vector anymore so clear it.
			// This may help garbage collection.

			raw_info.Clear();

			// Merge the two together into a new list of RawRowElement[]

			int merge_size = size1 + size2;
			RawRowElement[] elems = new RawRowElement[merge_size];
			int elems_index = 0;

			for (int i = 0; i < size1; ++i) {
				RawRowElement e = new RawRowElement();
				e.row_vals = new int[col_count];

				for (int n = 0; n < col_count; ++n) {
					e.row_vals[n] = merge1[n].row_set[i];
				}
				elems[elems_index] = e;
				++elems_index;
			}

			for (int i = 0; i < size2; ++i) {
				RawRowElement e = new RawRowElement();
				e.row_vals = new int[col_count];

				for (int n = 0; n < col_count; ++n) {
					e.row_vals[n] = merge2[n].row_set[i];
				}
				elems[elems_index] = e;
				++elems_index;
			}

			// Now sort the row elements into order.

			//TODO: SortUtil.QuickSort(elems);
			Array.Sort(elems);

			// Set up the 'raw_info' vector with the new RawTableElement[] removing
			// any duplicate rows.

			for (int i = 0; i < col_count; ++i) {
				RawTableElement e = merge1[i];
				e.row_set.Clear();
			}
			RawRowElement previous = null;
			RawRowElement current = null;
			for (int n = 0; n < merge_size; ++n) {
				current = elems[n];

				// Check that the current element in the set is not a duplicate of the
				// previous.

				if (previous == null || previous.CompareTo(current) != 0) {
					for (int i = 0; i < col_count; ++i) {
						merge1[i].row_set.AddInt(current.row_vals[i]);
					}
					previous = current;
				}
			}

			for (int i = 0; i < col_count; ++i) {
				raw_info.Add(merge1[i]);
			}

		}

		/// <summary>
		/// Removes any duplicate rows from this <see cref="RawTableInformation"/> object.
		/// </summary>
		internal void removeDuplicates() {

			// If no tables in duplicate then return

			if (raw_info.Count == 0) {
				return;
			}

			// Get the length of the first row set in the first table.  We assume that
			// the row set length is identical across each table in the Vector.

			RawTableElement elen = (RawTableElement)raw_info[0];
			int len = elen.row_set.Count;
			if (len == 0) {
				return;
			}

			// Create a new row element to sort.

			RawRowElement[] elems = new RawRowElement[len];
			int width = raw_info.Count;

			// Create an array of RawTableElement so we can quickly access the data

			RawTableElement[] rdup = new RawTableElement[width];
			raw_info.CopyTo(rdup);

			// Run through the data building up a new RawTableElement[] array with
			// the information in every raw span.

			for (int i = 0; i < len; ++i) {
				RawRowElement e = new RawRowElement();
				e.row_vals = new int[width];
				for (int n = 0; n < width; ++n) {
					e.row_vals[n] = rdup[n].row_set[i];
				}
				elems[i] = e;
			}

			// Now 'elems' it an array of individual RawRowElement objects which
			// represent each individual row in the table.

			// Now sort and remove duplicates to make up a new set.

			//TODO: SortUtil.QuickSort(elems);
			Array.Sort(elems);

			// Remove all elements from the raw_info Vector.

			raw_info.Clear();

			// Make a new set of RawTableElement[] objects

			RawTableElement[] table_elements = rdup;

			// Set up the 'raw_info' vector with the new RawTableElement[] removing
			// any duplicate rows.

			for (int i = 0; i < width; ++i) {
				table_elements[i].row_set.Clear();
			}
			RawRowElement previous = null;
			RawRowElement current = null;
			for (int n = 0; n < len; ++n) {
				current = elems[n];

				// Check that the current element in the set is not a duplicate of the
				// previous.

				if (previous == null || previous.CompareTo(current) != 0) {
					for (int i = 0; i < width; ++i) {
						table_elements[i].row_set.AddInt(current.row_vals[i]);
					}
					previous = current;
				}
			}

			for (int i = 0; i < width; ++i) {
				raw_info.Add(table_elements[i]);
			}

		}

	}

	/// <summary>
	/// A container class to hold the <see cref="DataTable"/> and 
	/// <see cref="IntegerVector"/> row set of a given table in the list.
	/// </summary>
	sealed class RawTableElement : IComparable {

		internal IRootTable table;
		internal IntegerVector row_set;

		public int CompareTo(Object o) {
			RawTableElement rte = (RawTableElement)o;
			return table.GetHashCode() - rte.table.GetHashCode();
		}

	}

	/// <summary>
	/// A container class to hold each row of a list of tables.
	/// </summary>
	/// <remarks>
	/// </remarks>
	sealed class RawRowElement : IComparable {

		internal int[] row_vals;

		public int CompareTo(Object o) {
			RawRowElement rre = (RawRowElement)o;

			int size = row_vals.Length;
			for (int i = 0; i < size; ++i) {
				int v1 = row_vals[i];
				int v2 = rre.row_vals[i];
				if (v1 != v2) {
					return v1 - v2;
				}
			}
			return 0;
		}
	}
}