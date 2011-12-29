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

namespace Deveel.Data.Caching {
	/// <summary>
	/// Represents a cache for accesses to the the data cells within a 
	/// <see cref="Table"/>.
	/// </summary>
	/// <remarks>
	/// Whenever a column/row index to a cell is accessed, the cache is first 
	/// checked. If the cell is not in the cache then it may go ahead and read 
	/// the cell from the file.
	/// <para>
	/// <b>Issue</b> We may need to keep track of memory used. Since a 
	/// <see cref="String"/> may use up much memory, we may need a cap on the 
	/// maximum size the cache can grow to. For example, we wouldn't want to 
	/// cache a large document. This could be handled at a higher level?
	/// </para>
	/// </remarks>
	public sealed class DataCellCache {
		/// <summary>
		/// A list of primes ordered from lowest to highest.
		/// </summary>
		private static readonly int[] PRIME_LIST = new int[]
		                                           {
		                                           	3001, 4799, 13999, 15377, 21803, 24247, 35083, 40531, 43669, 44263, 47387,
		                                           	50377, 57059, 57773, 59399, 59999, 75913, 96821, 140551, 149011, 175633,
		                                           	176389, 183299, 205507, 209771, 223099, 240259, 258551, 263909, 270761,
		                                           	274679, 286129, 290531, 296269, 298021, 300961, 306407, 327493, 338851,
		                                           	351037, 365489, 366811, 376769, 385069, 410623, 430709, 433729, 434509,
		                                           	441913, 458531, 464351, 470531, 475207, 479629, 501703, 510709, 516017,
		                                           	522211, 528527, 536311, 539723, 557567, 593587, 596209, 597451, 608897,
		                                           	611069, 642547, 670511, 677827, 679051, 688477, 696743, 717683, 745931,
		                                           	757109, 760813, 763957, 766261, 781559, 785597, 788353, 804493, 813559,
		                                           	836917, 854257, 859973, 883217, 884789, 891493, 902281, 910199, 915199,
		                                           	930847, 939749, 940483, 958609, 963847, 974887, 983849, 984299, 996211,
		                                           	999217, 1007519, 1013329, 1014287, 1032959, 1035829, 1043593, 1046459,
		                                           	1076171, 1078109, 1081027, 1090303, 1095613, 1098847, 1114037, 1124429,
		                                           	1125017, 1130191, 1159393, 1170311, 1180631, 1198609, 1200809, 1212943,
		                                           	1213087, 1226581, 1232851, 1287109, 1289867, 1297123, 1304987, 1318661,
		                                           	1331107, 1343161, 1345471, 1377793, 1385117, 1394681, 1410803, 1411987,
		                                           	1445261, 1460497, 1463981, 1464391, 1481173, 1488943, 1491547, 1492807,
		                                           	1528993, 1539961, 1545001, 1548247, 1549843, 1551001, 1553023, 1571417,
		                                           	1579099, 1600259, 1606153, 1606541, 1639751, 1649587, 1657661, 1662653,
		                                           	1667051, 1675273, 1678837, 1715537, 1718489, 1726343, 1746281, 1749107,
		                                           	1775489, 1781881, 1800157, 1806859, 1809149, 1826753, 1834607, 1846561,
		                                           	1849241, 1851991, 1855033, 1879931, 1891133, 1893737, 1899137, 1909513,
		                                           	1916599, 1917749, 1918549, 1919347, 1925557, 1946489, 1961551, 1965389,
		                                           	2011073, 2033077, 2039761, 2054047, 2060171, 2082503, 2084107, 2095099,
		                                           	2096011, 2112193, 2125601, 2144977, 2150831, 2157401, 2170141, 2221829,
		                                           	2233019, 2269027, 2270771, 2292449, 2299397, 2303867, 2309891, 2312407,
		                                           	2344301, 2348573, 2377007, 2385113, 2386661, 2390051, 2395763, 2422999,
		                                           	2448367, 2500529, 2508203, 2509841, 2513677, 2516197, 2518151, 2518177,
		                                           	2542091, 2547469, 2549951, 2556991, 2563601, 2575543, 2597629, 2599577,
		                                           	2612249, 2620003, 2626363, 2626781, 2636773, 2661557, 2674297, 2691571,
		                                           	2718269, 2725691, 2729381, 2772199, 2774953, 2791363, 2792939, 2804293,
		                                           	2843021, 2844911, 2851313, 2863519, 2880797, 2891821, 2897731, 2904887,
		                                           	2910251, 2928943, 2958341, 2975389
		                                           };

		/// <summary>
		/// The master cache.
		/// </summary>
		private readonly DCCache cache;

		/// <summary>
		/// The TransactionSystem that this cache is from.
		/// </summary>
		private readonly TransactionSystem system;

		/// <summary>
		/// The current size of the cache.
		/// </summary>
		private long currentCacheSize;

		/// <summary>
		/// The maximum size of a DataCell that is allowed to go in the cache.
		/// </summary>
		private int maxCellSize;

		/// <summary>
		/// Instantiate the <see cref="DataCellCache"/>.
		/// </summary>
		/// <param name="system"></param>
		/// <param name="systemCache"></param>
		/// <param name="maxCacheSize">The maximum size in bytes that the cache 
		/// is allowed to grow to (eg. 4000000).</param>
		/// <param name="maxCellSize">The maximum size of an object that can be 
		/// stored in the cache.</param>
		/// <param name="hashSize">The number of elements in the hash (should be 
		/// a prime number).</param>
		internal DataCellCache(TransactionSystem system, ICache systemCache, int maxCacheSize, int maxCellSize, int hashSize) {
			this.system = system;
			this.maxCellSize = maxCellSize;

			cache = new DCCache(this, systemCache, hashSize, maxCacheSize);
		}

		internal DataCellCache(TransactionSystem system, ICache systemCache, int maxCacheSize, int maxCellSize)
			: this(system, systemCache, maxCacheSize, maxCellSize, 88547) {
			// Good prime number hash size
		}

		/// <summary>
		/// Returns an estimation of the current cache size in bytes.
		/// </summary>
		public long CurrentCacheSize {
			get {
				lock (this) {
					return currentCacheSize;
				}
			}
		}

		/// <summary>
		/// Dynamically resizes the data cell cache so it can store more/less data.
		/// </summary>
		/// <param name="maxCacheSize"></param>
		/// <param name="maxCellSize"></param>
		/// <remarks>
		/// This is used to change cache dynamics at runtime.
		/// </remarks>
		public void AlterCacheDynamics(int maxCacheSize, int maxCellSize) {
			lock (this) {
				this.maxCellSize = maxCellSize;
				cache.SetCacheSize(maxCacheSize);
			}
		}

		/// <summary>
		/// Returns an approximation of the amount of memory taken by a 
		/// given <see cref="TObject"/>.
		/// </summary>
		/// <param name="cell"></param>
		/// <returns></returns>
		private static int AmountMemory(TObject cell) {
			return 16 + cell.ApproximateMemoryUse;
		}

		/// <summary>
		/// Adds a <see cref="TObject"/> on the cache for the given row/column 
		/// of the table.
		/// </summary>
		/// <param name="tableKey"></param>
		/// <param name="row"></param>
		/// <param name="column"></param>
		/// <param name="cell"></param>
		/// <remarks>
		/// Ignores any cells that are larger than the maximum size.
		/// </remarks>
		public void Set(int tableKey, int row, int column, TObject cell) {
			lock (this) {
				int memoryUse = AmountMemory(cell);
				if (memoryUse <= maxCellSize) {
					// Generate the key
					DCCacheKey key = new DCCacheKey(tableKey, (short)column, row);
					// If there is an existing object here, remove it from the cache and
					// update the current_cache_size.
					TObject removedCell = (TObject)cache.Remove(key);
					if (removedCell != null) {
						currentCacheSize -= AmountMemory(removedCell);
					}
					// Put the new entry in the cache
					cache.Set(key, cell);
					currentCacheSize += memoryUse;
				} else {
					// If the object is larger than the minimum object size that can be
					// cached, remove any existing entry (possibly smaller) from the cache.
					Remove(tableKey, row, column);
				}
			}
		}

		/// <summary>
		/// Gets a cell from the cache.
		/// </summary>
		/// <param name="tableKey"></param>
		/// <param name="row"></param>
		/// <param name="column"></param>
		/// <returns>
		/// Returns the value of the cell at the given coordinates, or <b>null</b> 
		/// if the row/column is not in the cache.
		/// </returns>
		public TObject Get(int tableKey, int row, int column) {
			lock (this) {
				return (TObject) cache.Get(new DCCacheKey(tableKey, (short) column, row));
			}
		}

		/// <summary>
		/// Removes a cell from the cache.
		/// </summary>
		/// <param name="tableKey"></param>
		/// <param name="row"></param>
		/// <param name="column"></param>
		/// <remarks>
		/// This is used when we need to notify the cache that an object has 
		/// become outdated.  This should be used when the cell has been removed 
		/// or changed.
		/// </remarks>
		/// <returns>
		/// Returns the cell that was removed, or <b>null</b> if there was no 
		/// cell at the given location.
		/// </returns>
		public TObject Remove(int tableKey, int row, int column) {
			lock (this) {
				TObject cell = (TObject)cache.Remove(new DCCacheKey(tableKey, (short) column, row));
				if (cell != null)
					currentCacheSize -= AmountMemory(cell);
				return cell;
			}
		}

		/// <summary>
		/// Completely wipe the cache of all entries.
		/// </summary>
		public void Clear() {
			lock (this) {
				if (cache.NodeCount == 0 && currentCacheSize != 0) {
					system.Logger.Error(this, "Assertion failed - if nodeCount = 0 then current_cache_size must also be 0.");
				}
				if (cache.NodeCount != 0) {
					cache.Clear();
					system.Stats.Increment("DataCellCache.total_cache_wipe");
				}
				currentCacheSize = 0;
			}
		}

		/// <summary>
		/// Reduce the cache size by the given amount.
		/// </summary>
		/// <param name="val"></param>
		private void ReduceCacheSize(long val) {
			currentCacheSize -= val;
		}

		// ---------- Primes ----------

		/// <summary>
		/// Returns a prime number from PRIME_LIST that is the closest prime 
		/// greater or equal to the given value.
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		internal static int ClosestPrime(int value) {
			for (int i = 0; i < PRIME_LIST.Length; ++i) {
				if (PRIME_LIST[i] >= value) {
					return PRIME_LIST[i];
				}
			}
			// Return the last prime
			return PRIME_LIST[PRIME_LIST.Length - 1];
		}

		#region DCCache

		/// <summary>
		/// An implementation of <see cref="Cache"/>.
		/// </summary>
		private sealed class DCCache : CacheWrapper {
			private readonly DataCellCache cache;

			/// <summary>
			/// The maximum size that the cache can grow to in bytes.
			/// </summary>
			private int maxCacheSize;

			public DCCache(DataCellCache cache, ICache systemCache, int cache_hash_size, int max_cache_size)
				: base(systemCache, cache_hash_size) {
				this.cache = cache;
				maxCacheSize = max_cache_size;
			}


			/// <summary>
			/// Used to dynamically alter the size of the cache.
			/// </summary>
			/// <param name="cache_size"></param>
			/// <remarks>
			/// May cause a cache clean if the size is over the limit.
			/// </remarks>
			public void SetCacheSize(int cache_size) {
				maxCacheSize = cache_size;
				CheckClean();
			}

			// ----- Overwritten from Cache -----

			protected override void CheckClean() {
				if (cache.CurrentCacheSize >= maxCacheSize) {
					// Update the current cache size (before we wiped).
					cache.system.Stats.Set((int) cache.CurrentCacheSize, "DataCellCache.current_cache_size");
					Clean();

					// The number of times we've cleared away old data cell nodes.
					cache.system.Stats.Increment("DataCellCache.cache_clean");
				}
			}

			protected override bool WipeMoreNodes() {
				return (cache.CurrentCacheSize >= (int) ((maxCacheSize*100L)/115L));
			}

			protected override void OnWipingNode(Object ob) {
				base.OnWipingNode(ob);

				// Update our memory indicator accordingly.
				TObject cell = (TObject)ob;
				cache.ReduceCacheSize(AmountMemory(cell));
			}

			protected override void OnGetWalks(long totalWalks, long totalGetOps) {
				int avg = (int) ((totalWalks*1000000L)/totalGetOps);
				cache.system.Stats.Set(avg, "DataCellCache.avg_hash_get_mul_1000000");
				cache.system.Stats.Set((int) cache.CurrentCacheSize, "DataCellCache.current_cache_size");
				cache.system.Stats.Set(NodeCount, "DataCellCache.current_node_count");
			}
		}

		#endregion

		#region DCCacheKey

		/// <summary>
		/// Struct that creates an object that hashes nicely 
		/// over the cache source.
		/// </summary>
		private struct DCCacheKey {
			private readonly short column;
			private readonly int row;
			private readonly int tableId;

			internal DCCacheKey(int tableId, short column, int row) {
				this.tableId = tableId;
				this.column = column;
				this.row = row;
			}

			public override bool Equals(Object ob) {
				DCCacheKey destKey = (DCCacheKey)ob;
				return row == destKey.row &&
				       column == destKey.column &&
				       tableId == destKey.tableId;
			}

			public override int GetHashCode() {
				// Yicks - this one is the best by far!
				return (((int) column + tableId + (row*189977))*50021) << 4;
			}
		}

		#endregion
	}
}