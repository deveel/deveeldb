//  
//  RowCache.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

using Deveel.Data.Util;

namespace Deveel.Data.Client {
    /// <summary>
    /// A cache that stores rows retrieved from the server in result set's.
    /// </summary>
    /// <remarks>
    /// This provides various mechanisms for determining the best rows to pick 
    /// out that haven't been cached, etc.
    /// </remarks>
	sealed class RowCache {
        /// <summary>
        /// The actual cache that stores the rows.
        /// </summary>
		private Cache row_cache;

        /// <summary>
        /// Constructs the cache.
        /// </summary>
        /// <param name="cache_size">The number of elements in the row cache.</param>
        /// <param name="max_size">The maximum size of the combined total of 
        /// all items in the cache.</param>
		internal RowCache(int cache_size, int max_size) {
			row_cache = new Cache(cache_size, cache_size, 20);
		}

        /// <summary>
        /// Requests a block of parts.
        /// </summary>
        /// <param name="result_block"></param>
        /// <param name="connection"></param>
        /// <param name="result_id"></param>
        /// <param name="row_index"></param>
        /// <param name="row_count"></param>
        /// <param name="col_count"></param>
        /// <param name="total_row_count"></param>
        /// <remarks>
        /// If the block can be completely retrieved from the cache then it is 
        /// done so.  Otherwise, it forwards the request for the rows onto the 
        /// connection object.
        /// </remarks>
        /// <returns></returns>
		internal ArrayList GetResultPart(ArrayList result_block,
			   DeveelDbConnection connection, int result_id, int row_index, int row_count,
			   int col_count, int total_row_count) {
			lock (this) {
				// What was requested....
				int orig_row_index = row_index;
				int orig_row_count = row_count;

				ArrayList rows = new ArrayList();

				// The top row that isn't found in the cache.
				bool found_notcached = false;
				// Look for the top row in the block that hasn't been cached
				for (int r = 0; r < row_count && !found_notcached; ++r) {
					int da_row = row_index + r;
					// Is the row in the cache?
					RowRef row_ref = new RowRef(result_id, da_row);
					// Not in cache so mark this as top row not in cache...
					CachedRow row = (CachedRow)row_cache.Get(row_ref);
					if (row == null) {
						row_index = da_row;
						if (row_index + row_count > total_row_count) {
							row_count = total_row_count - row_index;
						}
						found_notcached = true;
					} else {
						rows.Add(row);
					}
				}

				ArrayList rows2 = new ArrayList();
				if (found_notcached) {

					// Now work up from the bottom and find row that isn't in cache....
					found_notcached = false;
					// Look for the bottom row in the block that hasn't been cached
					for (int r = row_count - 1; r >= 0 && !found_notcached; --r) {
						int da_row = row_index + r;
						// Is the row in the cache?
						RowRef row_ref = new RowRef(result_id, da_row);
						// Not in cache so mark this as top row not in cache...
						CachedRow row = (CachedRow)row_cache.Get(row_ref);
						if (row == null) {
							if (row_index == orig_row_index) {
								row_index = row_index - (row_count - (r + 1));
								if (row_index < 0) {
									row_count = row_count + row_index;
									row_index = 0;
								}
							} else {
								row_count = r + 1;
							}
							found_notcached = true;
						} else {
							rows2.Insert(0, row);
						}
					}

				}

				// Some of it not in the cache...
				if (found_notcached) {
					//      Console.Out.WriteLine("REQUESTING: " + row_index + " - " + row_count);
					// Request a part of a result from the server (blocks)
					ResultPart block = connection.RequestResultPart(result_id,
																	row_index, row_count);

					int block_index = 0;
					for (int r = 0; r < row_count; ++r) {
						Object[] arr = new Object[col_count];
						int da_row = (row_index + r);
						int col_size = 0;
						for (int c = 0; c < col_count; ++c) {
							Object ob = block[block_index];
							++block_index;
							arr[c] = ob;
							col_size += ObjectTransfer.SizeOf(ob);
						}

						CachedRow cached_row = new CachedRow();
						cached_row.row = da_row;
						cached_row.row_data = arr;

						// Don't cache if it's over a certain size,
						if (col_size <= 3200) {
							row_cache.Set(new RowRef(result_id, da_row), cached_row);
						}
						rows.Add(cached_row);
					}

				}

				// At this point, the cached rows should be completely in the cache so
				// retrieve it from the cache.
				result_block.Clear();
				int low = orig_row_index;
				int high = orig_row_index + orig_row_count;
				for (int r = 0; r < rows.Count; ++r) {
					CachedRow row = (CachedRow)rows[r];
					// Put into the result block
					if (row.row >= low && row.row < high) {
						for (int c = 0; c < col_count; ++c) {
							result_block.Add(row.row_data[c]);
						}
					}
				}
				for (int r = 0; r < rows2.Count; ++r) {
					CachedRow row = (CachedRow)rows2[r];
					// Put into the result block
					if (row.row >= low && row.row < high) {
						for (int c = 0; c < col_count; ++c) {
							result_block.Add(row.row_data[c]);
						}
					}
				}

				// And return the result (phew!)
				return result_block;
			}
		}

        /// <summary>
        /// Flushes the complete contents of the cache.
        /// </summary>
		internal void Clear() {
			lock (this) {
				row_cache.Clear();
			}
		}





		// ---------- Inner classes ----------

        /// <summary>
        /// Used for the hash key in the cache.
        /// </summary>
		private sealed class RowRef {
            private int table_id;
            private int row;

			internal RowRef(int table_id, int row) {
				this.table_id = table_id;
				this.row = row;
			}

			public override int GetHashCode() {
				return (int)table_id + (row * 35331);
			}

			public override bool Equals(Object ob) {
				RowRef dest = (RowRef)ob;
				return (row == dest.row && table_id == dest.table_id);
			}
		}

        /// <summary>
        /// A cached row.
        /// </summary>
		private sealed class CachedRow {
			internal int row;
			internal Object[] row_data;
		}

	}
}