using System;
using System.Collections;

namespace Deveel.Data.Client {
	internal class RowCache {
		public RowCache(DeveelDbConnection connection, int cacheSize) {
			this.connection = connection;
			cache = new Hashtable(cacheSize, 0.2f);
		}

		private readonly DeveelDbConnection connection;
		private readonly Hashtable cache;

		public void Clear() {
			lock (this) {
				cache.Clear();
			}
		}

		public IList GetResultPart(IList resultBlock, int resultId, int rowIndex, int rowCount, int colCount, int totalRowCount) {
			lock (this) {
				// What was requested....
				int origRowIndex = rowIndex;
				int origRowCount = rowCount;

				ArrayList rows = new ArrayList();

				// The top row that isn't found in the cache.
				bool foundNotcached = false;
				// Look for the top row in the block that hasn't been cached
				for (int r = 0; r < rowCount && !foundNotcached; ++r) {
					int daRow = rowIndex + r;
					// Is the row in the cache?
					RowRef rowRef = new RowRef(resultId, daRow);
					// Not in cache so mark this as top row not in cache...
					CachedRow row = (CachedRow)cache[rowRef];
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

				ArrayList rows2 = new ArrayList();
				if (foundNotcached) {
					// Now work up from the bottom and find row that isn't in cache....
					foundNotcached = false;
					// Look for the bottom row in the block that hasn't been cached
					for (int r = rowCount - 1; r >= 0 && !foundNotcached; --r) {
						int daRow = rowIndex + r;
						// Is the row in the cache?
						RowRef rowRef = new RowRef(resultId, daRow);
						// Not in cache so mark this as top row not in cache...
						CachedRow row = (CachedRow)cache[rowRef];
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
					IList block = connection.Driver.GetResultPart(resultId, rowIndex, rowCount);

					int block_index = 0;
					for (int r = 0; r < rowCount; ++r) {
						Object[] arr = new Object[colCount];
						int da_row = (rowIndex + r);
						int col_size = 0;
						for (int c = 0; c < colCount; ++c) {
							Object ob = block[block_index];
							++block_index;
							arr[c] = ob;
							col_size += Driver.SizeOf(ob);
						}

						CachedRow cached_row = new CachedRow();
						cached_row.Row = da_row;
						cached_row.RowData = arr;

						// Don't cache if it's over a certain size,
						if (col_size <= 3200) {
							cache[new RowRef(resultId, da_row)] = cached_row;
						}
						rows.Add(cached_row);
					}

				}

				// At this point, the cached rows should be completely in the cache so
				// retrieve it from the cache.
				resultBlock.Clear();
				int low = origRowIndex;
				int high = origRowIndex + origRowCount;
				for (int r = 0; r < rows.Count; ++r) {
					CachedRow row = (CachedRow)rows[r];
					// Put into the result block
					if (row.Row >= low && row.Row < high) {
						for (int c = 0; c < colCount; ++c) {
							resultBlock.Add(row.RowData[c]);
						}
					}
				}
				for (int r = 0; r < rows2.Count; ++r) {
					CachedRow row = (CachedRow)rows2[r];
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

		private sealed class RowRef {
			private readonly int table_id;
			private readonly int row;

			public RowRef(int table_id, int row) {
				this.table_id = table_id;
				this.row = row;
			}

			public override int GetHashCode() {
				return table_id + (row * 35331);
			}

			public override bool Equals(Object ob) {
				RowRef dest = (RowRef)ob;
				return (row == dest.row && table_id == dest.table_id);
			}
		}

		private sealed class CachedRow {
			public int Row;
			public object[] RowData;
		}
	}
}