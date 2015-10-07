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

using Deveel.Data.Configuration;
using Deveel.Data;
using Deveel.Data.Sql;

namespace Deveel.Data.Caching {
	public sealed class TableCellCache : ITableCellCache, IConfigurable {
		private Cache cache;
		private long size;
		private bool configured;

		public const int DefaultHashSize = 88547;

		public int MaxCellSize { get; private set; }

		public long Size {
			get {
				lock (this) {
					return size;
				}
			}
		}

		private void ReduceCacheSize(long value) {
			size -= value;
		}

		void IConfigurable.Configure(IConfiguration config) {
			lock (this) {
				if (!configured) {
					var hashSize = DefaultHashSize;
					var maxSize = config.GetInt32(DatabaseConfigKeys.CellCacheMaxSize);
					MaxCellSize = config.GetInt32(DatabaseConfigKeys.CellCacheMaxCellSize);

					var baseCache = new SizeLimitedCache(maxSize);
					cache = new Cache(this, baseCache, hashSize, maxSize);

					configured = true;
				}	
			}
		}

		public void Set(CachedCell cell) {
			if (!configured)
				return;

			var value = cell.Value;

			if (!value.IsCacheable)
				return;

			lock (this) {
				int memoryUse = AmountMemory(value);
				if (memoryUse <= MaxCellSize) {
					// Generate the key
					var key = new CacheKey(cell.TableId, (int)cell.RowNumber, (short)cell.ColumnOffset);

					// If there is an existing object here, remove it from the cache and
					// update the current_cache_size.
					var removedCell = (DataObject)cache.Remove(key);
					if (!Equals(removedCell, null)) {
						size -= AmountMemory(removedCell);
					}

					// Put the new entry in the cache
					cache.Set(key, value);
					size += memoryUse;
				} else {
					// If the object is larger than the minimum object size that can be
					// cached, remove any existing entry (possibly smaller) from the cache.
					Remove(cell.TableId, cell.RowNumber, cell.ColumnOffset);
				}
			}
		}

		private void Remove(int tableId, long rowNumber, int columnOffset) {
			if (!configured)
				return;

			lock (this) {
				var cell = cache.Remove(new CacheKey(tableId, (int)rowNumber, (short)columnOffset));
				if (cell != null)
					size -= AmountMemory((DataObject) cell);
			}
		}

		private static int AmountMemory(DataObject value) {
			return 16 + value.CacheUsage;
		}

		public bool TryGetValue(RowId rowId, int columnIndex, out DataObject value) {
			if (!configured) {
				value = null;
				return false;
			}

			lock (this) {
				var tableKey = rowId.TableId;
				var row = rowId.RowNumber;
				object obj;
				if (!cache.TryGet(new CacheKey(tableKey, row, (short)columnIndex), out obj)) { 
					value = null;
					return false;
				}

				value = (DataObject) obj;
				return true;
			}
		}

		public void Remove(RowId rowId, int columnIndex) {
			Remove(rowId.TableId, rowId.RowNumber, columnIndex);
		}

		public void Clear() {
			if (!configured)
				return;

			lock (this) {
				if (cache.NodeCount == 0 && Size != 0) {
					// TODO: Raise an error
				}
				if (cache.NodeCount != 0) {
					cache.Clear();
					// TODO: Register the statistics
				}

				size = 0;
			}
		}

		#region Cache

		class Cache : CacheAdapter {
			private readonly TableCellCache tableCache;
			private int hashSize;

			public Cache(TableCellCache tableCache, ICache baseCache, int hashSize, int maxSize) 
				: base(baseCache) {
				this.tableCache = tableCache;
				this.hashSize = hashSize;
			}

			public void ChangeSize(int newSize) {
				hashSize = newSize;
				CheckClean();
			}

			protected override void CheckClean() {
				if (tableCache.Size >= hashSize) {
					// TODO: Register the statistics

					Clean();

					//TODO: Register the statistics
				}
			}

			protected override bool WipeMoreNodes() {
				return (tableCache.Size >= (int)((hashSize * 100L) / 115L));
			}

			protected override void OnWipingNode(object ob) {
				base.OnWipingNode(ob);

				// Update our memory indicator accordingly.
				var value = (DataObject)ob;
				tableCache.ReduceCacheSize(AmountMemory(value));
			}

			protected override void OnGetWalks(long totalWalks, long totalGetOps) {
				// TODO: Register the statistics ...
				base.OnGetWalks(totalWalks, totalGetOps);
			}
		}

		#endregion

		#region CacheKey

		class CacheKey : IEquatable<CacheKey> {
			private readonly short column;
			private readonly int row;
			private readonly int tableId;

			public CacheKey(int tableId, int row, short column) {
				this.tableId = tableId;
				this.row = row;
				this.column = column;
			}

			public override bool Equals(object obj) {
				return Equals((CacheKey)obj);
			}

			public override int GetHashCode() {
				// Yicks - this one is the best by far!
				return (((int)column + tableId + (row * 189977)) * 50021) << 4;
			}

			public bool Equals(CacheKey other) {
				return row == other.row &&
					   column == other.column &&
					   tableId == other.tableId;
			}
		}

		#endregion
	}
}
