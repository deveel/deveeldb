// 
//  Copyright 2010-2014  Deveel
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
using System.Collections;
using System.Collections.Generic;

using Deveel.Data.Caching;
using Deveel.Data.Protocol;

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
		private readonly Cache rowCache;

        /// <summary>
        /// Constructs the cache.
        /// </summary>
        /// <param name="cacheSize">The number of elements in the row cache.</param>
        /// <param name="maxSize">The maximum size of the combined total of 
        /// all items in the cache.</param>
		internal RowCache(int cacheSize, int maxSize) {
			rowCache = new MemoryCache(cacheSize, maxSize, 20);
		}

        /// <summary>
        /// Requests a block of parts.
        /// </summary>
        /// <param name="resultBlock"></param>
        /// <param name="connection"></param>
        /// <param name="resultId"></param>
        /// <param name="rowIndex"></param>
        /// <param name="rowCount"></param>
        /// <param name="colCount"></param>
        /// <param name="totalRowCount"></param>
        /// <remarks>
        /// If the block can be completely retrieved from the cache then it is 
        /// done so.  Otherwise, it forwards the request for the rows onto the 
        /// connection object.
        /// </remarks>
        /// <returns></returns>
		internal List<object> GetResultPart(List<object> resultBlock, DeveelDbConnection connection, int resultId, 
			int rowIndex, int rowCount, int colCount, int totalRowCount) {
			lock (this) {
				// What was requested....
				int origRowIndex = rowIndex;
				int origRowCount = rowCount;

				var rows = new List<CachedRow>();

				// The top row that isn't found in the cache.
				bool foundNotcached = false;
				// Look for the top row in the block that hasn't been cached
				for (int r = 0; r < rowCount && !foundNotcached; ++r) {
					int daRow = rowIndex + r;
					// Is the row in the cache?
					var rowRef = new RowRef(resultId, daRow);
					// Not in cache so mark this as top row not in cache...
					var row = (CachedRow)rowCache.Get(rowRef);
					if (row == null) {
						rowIndex = daRow;
						if (rowIndex + rowCount > totalRowCount) {
							rowCount = totalRowCount - rowIndex;
						}
						foundNotcached = true;
					} else {
						rows.Add(row);
					}
				}

				var rows2 = new List<CachedRow>();
				if (foundNotcached) {

					// Now work up from the bottom and find row that isn't in cache....
					foundNotcached = false;
					// Look for the bottom row in the block that hasn't been cached
					for (int r = rowCount - 1; r >= 0 && !foundNotcached; --r) {
						int daRow = rowIndex + r;
						// Is the row in the cache?
						var rowRef = new RowRef(resultId, daRow);
						// Not in cache so mark this as top row not in cache...
						var row = (CachedRow)rowCache.Get(rowRef);
						if (row == null) {
							if (rowIndex == origRowIndex) {
								rowIndex = rowIndex - (rowCount - (r + 1));
								if (rowIndex < 0) {
									rowCount = rowCount + rowIndex;
									rowIndex = 0;
								}
							} else {
								rowCount = r + 1;
							}
							foundNotcached = true;
						} else {
							rows2.Insert(0, row);
						}
					}
				}

				// Some of it not in the cache...
				if (foundNotcached) {
					// Request a part of a result from the server (blocks)
					ResultPart block = connection.RequestResultPart(resultId, rowIndex, rowCount);

					int blockIndex = 0;
					for (int r = 0; r < rowCount; ++r) {
						var arr = new Object[colCount];
						int daRow = (rowIndex + r);
						int colSize = 0;
						for (int c = 0; c < colCount; ++c) {
							object ob = block[blockIndex];
							++blockIndex;
							arr[c] = ob;
							colSize += ObjectTransfer.SizeOf(ob);
						}

						var cachedRow = new CachedRow();
						cachedRow.Row = daRow;
						cachedRow.RowData = arr;

						// Don't cache if it's over a certain size,
						if (colSize <= 3200) {
							rowCache.Set(new RowRef(resultId, daRow), cachedRow);
						}
						rows.Add(cachedRow);
					}

				}

				// At this point, the cached rows should be completely in the cache so
				// retrieve it from the cache.
				resultBlock.Clear();
				int low = origRowIndex;
				int high = origRowIndex + origRowCount;
				foreach (CachedRow row in rows) {
					// Put into the result block
					if (row.Row >= low && row.Row < high) {
						for (int c = 0; c < colCount; ++c) {
							resultBlock.Add(row.RowData[c]);
						}
					}
				}
				foreach (CachedRow row in rows2) {
					// Put into the result block
					if (row.Row >= low && row.Row < high) {
						for (int c = 0; c < colCount; ++c) {
							resultBlock.Add(row.RowData[c]);
						}
					}
				}

				// And return the result (phew!)
				return resultBlock;
			}
		}

        /// <summary>
        /// Flushes the complete contents of the cache.
        /// </summary>
		internal void Clear() {
			lock (this) {
				rowCache.Clear();
			}
		}

		// ---------- Inner classes ----------

        /// <summary>
        /// Used for the hash key in the cache.
        /// </summary>
		private sealed class RowRef {
            private readonly int tableId;
            private readonly int row;

			internal RowRef(int tableId, int row) {
				this.tableId = tableId;
				this.row = row;
			}

			public override int GetHashCode() {
				return (int)tableId + (row * 35331);
			}

			public override bool Equals(Object ob) {
				var dest = (RowRef)ob;
				return (row == dest.row && tableId == dest.tableId);
			}
		}

        /// <summary>
        /// A cached row.
        /// </summary>
		private sealed class CachedRow {
			public int Row;
			public object[] RowData;
		}

	}
}