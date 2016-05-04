// 
//  Copyright 2010-2016 Deveel
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
using Deveel.Data.Diagnostics;
using Deveel.Data.Sql;

namespace Deveel.Data.Caching {
	public sealed class TableCellCache : ITableCellCache, IDisposable {
		private Cache cache;
		private long size;

		public const int DefaultHashSize = 88547;
#if X64
		public const int DefaultMaxSize = 1024*1024;
#else
		public const int DefaultMaxSize = 512*512;
#endif

		public const int DefaultMaxCellSize = 1024*64;

		public TableCellCache(ISystemContext context) {
			Configure(context);
		}

		~TableCellCache() {
			Dispose(false);
		}

		public int MaxCellSize { get; private set; }

		public ISystemContext Context { get; private set; }

		public long Size {
			get {
				lock (this) {
					return size;
				}
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (cache != null)
					cache.Dispose();
			}

			cache = null;
		}

		private void ReduceCacheSize(long value) {
			size -= value;
		}

		private void Configure(ISystemContext context) {
			var config = context.ResolveService<IConfiguration>();
			var hashSize = DefaultHashSize;
			var maxSize = config.GetInt32("system.tableCellCache.maxSize", DefaultMaxSize);
			MaxCellSize = config.GetInt32("system.tableCellCache.maxCellSize", DefaultMaxCellSize);

			var baseCache = new SizeLimitedCache(maxSize);
			cache = new Cache(this, baseCache, hashSize);

			Context = context;
		}

		public void Set(CachedCell cell) {
			var value = cell.Value;

			if (!value.IsCacheable)
				return;

			lock (this) {
				int memoryUse = AmountMemory(value);
				if (memoryUse <= MaxCellSize) {
					// Generate the key
					var key = new CacheKey(cell.Database, cell.TableId, (int)cell.RowNumber, (short)cell.ColumnOffset);

					// If there is an existing object here, remove it from the cache and
					// update the current_cache_size.
					var removedCell = (Field)cache.Remove(key);
					if (!Equals(removedCell, null)) {
						size -= AmountMemory(removedCell);
					}

					// Put the new entry in the cache
					cache.Set(key, value);
					size += memoryUse;
				} else {
					// If the object is larger than the minimum object size that can be
					// cached, remove any existing entry (possibly smaller) from the cache.
					Remove(cell.Database, cell.TableId, cell.RowNumber, cell.ColumnOffset);
				}
			}
		}

		private void Remove(string database, int tableId, long rowNumber, int columnOffset) {
			lock (this) {
				var cell = cache.Remove(new CacheKey(database, tableId, (int)rowNumber, (short)columnOffset));
				if (cell != null)
					size -= AmountMemory((Field) cell);
			}
		}

		private static int AmountMemory(Field value) {
			return 16 + value.CacheUsage;
		}

		public bool TryGetValue(CellKey key, out Field value) {
			lock (this) {
				var database = key.Database;
				var tableKey = key.RowId.TableId;
				var row = key.RowId.RowNumber;
				var columnIndex = key.ColumnOffset;

				object obj;
				if (!cache.TryGet(new CacheKey(database, tableKey, row, (short)columnIndex), out obj)) { 
					value = null;
					return false;
				}

				value = (Field) obj;
				return true;
			}
		}

		public void Remove(CellKey key) {
			Remove(key.Database, key.RowId.TableId, key.RowId.RowNumber, key.ColumnOffset);
		}

		public void Clear() {
			lock (this) {
				if (cache.NodeCount == 0 && Size != 0) {
					Context.OnWarning("The node count is 0 and the cache size is 0");
				}
				if (cache.NodeCount != 0) {
					cache.Clear();

					Context.OnEvent(new CounterEvent("tableCellCache.totalWipeCount"));
				}

				size = 0;
			}
		}

#region Cache

		class Cache : CacheAdapter {
			private TableCellCache tableCache;
			private int hashSize;

			public Cache(TableCellCache tableCache, ICache baseCache, int hashSize) 
				: base(baseCache) {
				this.tableCache = tableCache;
				this.hashSize = hashSize;
			}

			protected override void Dispose(bool disposing) {
				tableCache = null;
				base.Dispose(disposing);
			}

			public void ChangeSize(int newSize) {
				hashSize = newSize;
				CheckClean();
			}

			protected override void CheckClean() {
				if (tableCache.Size >= hashSize) {
					tableCache.Context.OnEvent(new CounterEvent("tableCellCache.currentSize", tableCache.Size));

					Clear();

					tableCache.Context.OnEvent(new CounterEvent("tableCellCache.cleanCount"));
				}
			}

			protected override bool WipeMoreNodes() {
				return (tableCache.Size >= (int)((hashSize * 100L) / 115L));
			}

			protected override void OnWipingNode(object ob) {
				base.OnWipingNode(ob);

				// Update our memory indicator accordingly.
				var value = (Field)ob;
				tableCache.ReduceCacheSize(AmountMemory(value));
			}

			protected override void OnGetWalks(long totalWalks, long totalGetOps) {
				var avg = (int)((totalWalks * 1000000L) / totalGetOps);
				tableCache.Context.OnEvent(new CounterEvent("tableCellCache.avgHashGet.multiple100000", avg));
				tableCache.Context.OnEvent(new CounterEvent("tableCellCache.currentSize", tableCache.Size));
				tableCache.Context.OnEvent(new CounterEvent("tableCellCache.nodeCount", NodeCount));
				
				base.OnGetWalks(totalWalks, totalGetOps);
			}
		}

		#endregion

		#region CacheKey

		class CacheKey : IEquatable<CacheKey> {
			private readonly string database;
			private readonly short column;
			private readonly int row;
			private readonly int tableId;

			public CacheKey(string database, int tableId, int row, short column) {
				this.database = database;
				this.tableId = tableId;
				this.row = row;
				this.column = column;
			}

			public override bool Equals(object obj) {
				return Equals((CacheKey)obj);
			}

			public override int GetHashCode() {
				// Yicks - this one is the best by far!
				return (database.GetHashCode() + (((int)column + tableId + (row * 189977)) * 50021) << 4);
			}

			public bool Equals(CacheKey other) {
				return database.Equals(other.database) &&
				       row == other.row &&
				       column == other.column &&
				       tableId == other.tableId;
			}
		}

#endregion
	}
}
