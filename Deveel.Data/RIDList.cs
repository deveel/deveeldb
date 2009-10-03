// 
//  RIDList.cs
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
using System.IO;

using Deveel.Data.Collections;
using Deveel.Diagnostics;

namespace Deveel.Data {
	/// <summary>
	/// This is an optimization to help sorting over a column in a table.
	/// </summary>
	/// <remarks>
	/// It is an aid for sorting rows in a query without having to resort 
	/// to cell lookup. It uses memory to speed up sorting.
	/// <para>
	/// Sorting data is a central part of any database system. This object
	/// maintains a list of values that represent each cell in a column
	/// relationally.
	/// </para>
	/// <para>
	/// A RID list is a set of integer values that represents a column relationally.
	/// </para>
	/// </remarks>
	/// <example>
	/// For example, consider the following data in a column:
	/// <code>
	/// { 'a', 'g', 'i', 'b', 'a' }
	/// </code>
	/// The above column data could be represented in a RowId list as:
	/// <code>
	/// { 1, 3, 4, 2, 1 }
	/// </code>
	/// If <i>c</i> is inserted into the above list, there is not an integer 
	/// value that we can use to represent this cell. In this case, the 
	/// RID list is renumbered to make room for the insertion.
	/// </example>
	sealed class RIDList {

		/// <summary>
		/// The TransactionSystem that we are in.
		/// </summary>
		private readonly TransactionSystem system;

		/// <summary>
		/// The master table for the column this is in.
		/// </summary>
		private readonly MasterTableDataSource master_table;

		/// <summary>
		/// The TableName of the table.
		/// </summary>
		private readonly TableName table_name;

		/// <summary>
		/// The name of the column of this rid list.
		/// </summary>
		private readonly String column_name;

		/// <summary>
		/// The column in the master table.
		/// </summary>
		private readonly int column;

		/// <summary>
		/// The sorted list of rows in this set.
		/// </summary>
		/// <remarks>
		/// This is sorted from min to max (not sorted by row number - 
		/// sorted by entity row value).
		/// </remarks>
		private BlockIntegerList set_list;

		/// <summary>
		/// The contents of our list.
		/// </summary>
		private IntegerVector rid_list;

		/// <summary>
		/// The difference between each hash when the uid_list was last 
		/// created or rehashed.
		/// </summary>
		private int hash_rid_difference;

		/// <summary>
		/// The IIndexComparer that we use to refer elements in the set to 
		/// actual data objects.
		/// </summary>
		private IIndexComparer set_comparator;

		/// <summary>
		/// Set to true if this list has been fully built.
		/// </summary>
		private bool is_built;

		/// <summary>
		/// The RID list build state.
		/// </summary>
		/// <remarks>
		/// <code>
		/// 0 - list not built.
		/// 1 - stage 1 (set_list being built).
		/// 2 - state 2 (rid_list being built).
		/// 3 - pending modifications.
		/// 4 - finished
		/// </code>
		/// </remarks>
		private int build_state = 0;

		// A list of modifications made to the index while it is being built.
		private IntegerVector concurrent_modification_info;
		private ArrayList concurrent_modification_data;
		private Object modification_lock = new Object();

		/// <summary>
		/// Set to true if a request to build the rid list is on the event dispatcher.
		/// </summary>
		private bool request_processing = false;

		RIDList(MasterTableDataSource master_table, int column) {
			//    rid_list = new IntegerVector();
			this.master_table = master_table;
			this.system = master_table.System;
			this.column = column;

			DataTableDef table_def = master_table.DataTableDef;
			table_name = table_def.TableName;
			column_name = table_def[column].Name;

			is_built = false;
			SetupComparator();
		}

		/// <summary>
		/// Returns a IDebugLogger object that we can use to log debug messages.
		/// </summary>
		/*
		TODO:
		public IDebugLogger Debug {
			get { return master_table.Debug; }
		}
		*/

		/// <summary>
		/// Sets the internal comparator that enables us to sort and lookup 
		/// on the data in this column.
		/// </summary>
		private void SetupComparator() {
			set_comparator = new IndexComparatorImplA(this);
		}

		private class IndexComparatorImplA : IIndexComparer {
			private readonly RIDList rid_list;

			public IndexComparatorImplA(RIDList ridList) {
				rid_list = ridList;
			}

			private int InternalCompare(int index, TObject cell2) {
				TObject cell1 = rid_list.GetCellContents(index);
				return cell1.CompareTo(cell2);
			}

			public int Compare(int index, Object val) {
				return InternalCompare(index, (TObject)val);
			}
			public int Compare(int index1, int index2) {
				TObject cell = rid_list.GetCellContents(index2);
				return InternalCompare(index1, cell);
			}

			#region Implementation of IComparer

			public int Compare(object x, object y) {
				return y is int ? Compare((int) x, (int) y) : Compare((int) x, y);
			}

			#endregion
		}

		/// <summary>
		/// Gets the cell at the given row in the column of the master table.
		/// </summary>
		/// <param name="row"></param>
		/// <returns></returns>
		private TObject GetCellContents(int row) {
			return master_table.GetCellContents(column, row);
		}

		/// <summary>
		/// Calculates the <see cref="hash_rid_difference"/> variable. 
		/// This dictates the difference between hashing entries.
		/// </summary>
		/// <param name="size"></param>
		private void CalcHashRidDifference(int size) {
			if (size == 0) {
				hash_rid_difference = 32;
			} else {
				hash_rid_difference = (65536 * 4096) / size;
				if (hash_rid_difference > 16384) {
					hash_rid_difference = 16384;
				} else if (hash_rid_difference < 8) {
					hash_rid_difference = 8;
				}
			}

			//    hash_rid_difference = 2;
			//    Console.Out.WriteLine(hash_rid_difference);
		}


		/// <summary>
		/// Rehashes the entire rid list.
		/// </summary>
		/// <param name="old_rid_place"></param>
		/// <remarks>
		/// This goes through the entire list from first sorted entry to 
		/// last and spaces out each rid so that there's 16 numbers between 
		/// each entry.
		/// </remarks>
		/// <returns></returns>
		private int rehashRIDList(int old_rid_place) {
			CalcHashRidDifference(set_list.Count);

			int new_rid_place = -1;

			int cur_rid = 0;
			int old_rid = 0;
			IIntegerIterator iterator = set_list.GetIterator();

			while (iterator.MoveNext()) {
				int row_index = iterator.next();
				if (row_index >= 0 && row_index < rid_list.Count) {
					int old_value = rid_list[row_index];
					int new_value;

					if (old_value == 0) {
						cur_rid += hash_rid_difference;
						new_rid_place = cur_rid;
					} else {
						if (old_value != old_rid) {
							old_rid = old_value;
							cur_rid += hash_rid_difference;
							new_value = cur_rid;
						} else {
							new_value = cur_rid;
						}
						rid_list.PlaceIntAt(new_value, row_index);
					}
				}
			}

			if (new_rid_place == -1) {
				throw new ApplicationException(
						  "Post condition not correct - new_rid_place shouldn't be -1");
			}

			system.Stats.Increment("RIDList.rehash_rid_table");

			return new_rid_place;
		}


		/// <summary>
		/// Algorithm for inserting a new row into the rid table.
		/// </summary>
		/// <param name="cell">The value to insert.</param>
		/// <param name="row">The row index for the value.</param>
		/// <remarks>
		/// For most cases this should be a very fast method.
		/// <para>
		/// <b>Note</b> This must never be called from anywhere except inside
		/// <see cref="MasterTableDataSource"/>.
		/// </para>
		/// </remarks>
		internal void InsertRID(TObject cell, int row) {
			// NOTE: We are guarenteed to be synchronized on master_table when this
			//   is called.

			lock (modification_lock) {

				// If state isn't pre-build or finished, then note this modification.
				if (build_state > 0 && build_state < 4) {
					concurrent_modification_info.AddInt(1);
					concurrent_modification_info.AddInt(row);
					concurrent_modification_data.Add(cell);
					return;
				}

				// Only register if this list has been created.
				if (rid_list == null) {
					return;
				}

			}

			// Place a zero to mark the new row
			rid_list.PlaceIntAt(0, row);

			// Insert this into the set_list.
			set_list.InsertSort(cell, row, set_comparator);

			int given_rid = -1;
			TObject previous_cell;

			// The index of this cell in the list
			int set_index = set_list.SearchLast(cell, set_comparator);

			if (set_list[set_index] != row) {
				throw new ApplicationException(
						   "set_list.SearchLast(cell) didn't turn up expected row.");
			}

			int next_set_index = set_index + 1;
			if (next_set_index >= set_list.Count) {
				next_set_index = -1;
			}
			int previous_set_index = set_index - 1;

			int next_rid;
			if (next_set_index > -1) {
				next_rid = rid_list[set_list[next_set_index]];
			} else {
				if (previous_set_index > -1) {
					// If at end and there's a previous set then use that as the next
					// rid.
					next_rid = rid_list[set_list[previous_set_index]] +
							  (hash_rid_difference * 2);
				} else {
					next_rid = (hash_rid_difference * 2);
				}
			}
			int previous_rid;
			if (previous_set_index > -1) {
				previous_rid = rid_list[set_list[previous_set_index]];
			} else {
				previous_rid = 0;
			}

			// Are we the same as the previous or next cell in the list?
			if (previous_set_index > -1) {
				previous_cell = GetCellContents(set_list[previous_set_index]);
				if (previous_cell.CompareTo(cell) == 0) {
					given_rid = previous_rid;
				}
			}

			// If not given a rid yet,
			if (given_rid == -1) {
				if (previous_rid + 1 == next_rid) {
					// There's no room so we have to rehash the rid list.
					given_rid = rehashRIDList(next_rid);
				} else {
					given_rid = ((next_rid + 1) + (previous_rid - 1)) / 2;
				}
			}

			// Finally (!!) - set the rid for this row.
			rid_list.PlaceIntAt(given_rid, row);

		}

		/// <summary>
		/// Removes an entry from the given row.
		/// </summary>
		/// <param name="row"></param>
		/// <remarks>
		/// This <b>must</b> only be called when the row is permanently 
		/// removed from the table (eg. by the row garbage collector).
		/// <para>
		/// <b>Note:</b> This must never be called from anywhere except inside
		/// <see cref="MasterTableDataSource"/>.
		/// </para>
		/// </remarks>
		internal void RemoveRID(int row) {
			// NOTE: We are guarenteed to be synchronized on master_table when this
			//   is called.

			lock (modification_lock) {

				// If state isn't pre-build or finished, then note this modification.
				if (build_state > 0 && build_state < 4) {
					concurrent_modification_info.AddInt(2);
					concurrent_modification_info.AddInt(row);
					return;
				}

				// Only register if this list has been created.
				if (rid_list == null) {
					return;
				}

			}

			try {
				// Remove from the set_list index.
				TObject cell = GetCellContents(row);
				int removed = set_list.RemoveSort(cell, row, set_comparator);
			} catch (Exception) {
				Console.Error.WriteLine("RIDList: " + table_name + "." + column_name);
				throw;
			}

		}

		/// <summary>
		/// Requests that a rowid list should be built for the underlying 
		/// column.
		/// </summary>
		/// <remarks>
		/// The list will be built on the database dispatcher thread.
		/// </remarks>
		internal void requestBuildRIDList() {
			if (!IsBuilt) {
				if (!request_processing) {
					request_processing = true;
					// Wait 10 seconds to build rid list.
					system.PostEvent(10000, system.CreateEvent(new EventHandler(CreateRIDCacheEvent)));
				}
			}
		}

		private void CreateRIDCacheEvent(object sender, EventArgs e) {
			CreateRIDCache();
		}

		/// <summary>
		/// If <see cref="rid_list"/> is null then create it now.
		/// </summary>
		/// <remarks>
		/// This must never be called from anywhere except inside <see cref="MasterTableDataSource"/>.
		/// </remarks>
		private void CreateRIDCache() {

			try {

				// If the master table is closed then return
				// ISSUE: What if this happens while we are constructing the list?
				if (master_table.IsClosed) {
					return;
				}

				DateTime time_start = DateTime.Now;
				TimeSpan time_took;
				int rid_list_size;

				int set_size;

				lock (master_table) {
					lock (modification_lock) {

						if (is_built) {
							return;
						}

						// Set the build state
						build_state = 1;
						concurrent_modification_info = new IntegerVector();
						concurrent_modification_data = new ArrayList();

						// The set_list (complete index of the master table).
						set_size = master_table.RawRowCount;
						set_list = new BlockIntegerList();
						// Go through the master table and build set_list.
						for (int r = 0; r < set_size; ++r) {
							if (!master_table.RecordDeleted(r)) {
								TObject cell = GetCellContents(r);
								set_list.InsertSort(cell, r, set_comparator);
							}
						}
						// Now we have a complete/current index, including uncommitted,
						// and committed added and removed rows, of the given column

						// Add a root Lock to the table
						master_table.AddRootLock();

					} // lock (modification_lock)
				} // lock master_table

				try {

					// Go through and work out the rid values for the list.  We know
					// that 'set_list' is correct and no entries can be deleted from it
					// until we relinquish the root Lock.

					CalcHashRidDifference(set_size);

					rid_list = new IntegerVector(set_size + 128);

					// Go through 'set_list'.  All entries that are equal are given the
					// same rid.
					if (set_list.Count > 0) {   //set_size > 0) {
						int cur_rid = hash_rid_difference;
						IIntegerIterator iterator = set_list.GetIterator();
						int row_index = iterator.next();
						TObject last_cell = GetCellContents(row_index);
						rid_list.PlaceIntAt(cur_rid, row_index);

						while (iterator.MoveNext()) {
							row_index = iterator.next();
							TObject cur_cell = GetCellContents(row_index);
							int cmp = cur_cell.CompareTo(last_cell);
							if (cmp > 0) {
								cur_rid += hash_rid_difference;
							} else if (cmp < 0) {  // ASSERTION
								// If current cell is less than last cell then the list ain't
								// sorted!
								throw new ApplicationException("Internal Database Error: Index is corrupt " +
												" - InsertSearch list is not sorted.");
							}
							rid_list.PlaceIntAt(cur_rid, row_index);

							last_cell = cur_cell;
						}
					}

					// Final stage, insert final changes,
					// We Lock the master_table so we are guarenteed no changes to the
					// table can happen during the final stage.
					lock (master_table) {
						lock (modification_lock) {
							build_state = 4;

							// Make any modifications to the list that occured during the time
							// we were building the RID list.
							int mod_size = concurrent_modification_info.Count;
							int i = 0;
							int m_data = 0;
							int insert_count = 0;
							int remove_count = 0;
							while (i < mod_size) {
								int type = concurrent_modification_info[i];
								int row = concurrent_modification_info[i + 1];
								// An insert
								if (type == 1) {
									TObject cell =
												(TObject)concurrent_modification_data[m_data];
									InsertRID(cell, row);
									++m_data;
									++insert_count;
								}
									// A remove
								else if (type == 2) {
									RemoveRID(row);
									++remove_count;
								} else {
									throw new ApplicationException("Unknown modification type.");
								}

								i += 2;
							}

							if (remove_count > 0) {
								Debug.Write(DebugLevel.Error, this,
									"Assertion failed: It should not be possible to remove " +
									"rows during a root Lock when building a RID list.");
							}

							concurrent_modification_info = null;
							concurrent_modification_data = null;

							// Log the time it took
							time_took = DateTime.Now - time_start;
							rid_list_size = rid_list.Count;

							is_built = true;

						}
					} // synchronized (modification_lock)

				} finally {
					// Must guarentee we remove the root Lock from the master table
					master_table.RemoveRootLock();
				}

				Debug.Write(DebugLevel.Message, this,
						 "RID List " + table_name.ToString() + "." + column_name +
						 " Initial Size = " + rid_list_size);
				Debug.Write(DebugLevel.Message, this,
						 "RID list built in " + time_took + "ms.");

				// The number of rid caches created.
				system.Stats.Increment(
									 "{session} RIDList.rid_caches_created");
				// The total size of all rid indices that we have created.
				system.Stats.Add(rid_list_size,
									 "{session} RIDList.rid_indices");

			} catch (IOException e) {
				throw new ApplicationException("IO Error: " + e.Message);
			}

		}

		/// <summary>
		/// Quick way of determining if the RID list has been built.
		/// </summary>
		private bool IsBuilt {
			get {
				lock (modification_lock) {
					return is_built;
				}
			}
		}
	}
}