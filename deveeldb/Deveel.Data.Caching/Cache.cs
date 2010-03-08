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
using System.Collections;

namespace Deveel.Data.Caching {
	///<summary>
	/// Represents a cache of Objects.
	///</summary>
	/// <remarks>
	/// A <see cref="Cache"/> is similar to a <see cref="Hashtable"/>, in that you can 
	/// <see cref="Hashtable.Add">add</see> and <see cref="Hashtable.this">get</see> 
	/// objects from the container given some key. However a cache may remove objects 
	/// from the container when it becomes too full.
	/// <para>
	/// The cache scheme uses a doubly linked-list hashtable.  The most recently
	/// accessed objects are moved to the start of the list.  The end elements in
	/// the list are wiped if the cache becomes too full.
	/// </para>
	/// </remarks>
	public abstract class Cache : ICache {
		/// <summary>
		/// The maximum number of DataCell objects that can be stored in 
		/// the cache at any one time.
		/// </summary>
		private readonly int max_cache_size;

		/// <summary>
		/// The current cache size.
		/// </summary>
		private int current_cache_size;

		/// <summary>
		/// The number of nodes that should be left available when the cache becomes
		/// too full and a clean up operation occurs.
		/// </summary>
		private readonly int wipe_to;

		///<summary>
		/// Constructs the <see cref="Cache"/> with a maximum size it can grow
		/// and a percentage of the cache that is wiped when it becomes too full.
		///</summary>
		///<param name="max_size"></param>
		///<param name="clean_percentage"></param>
		///<exception cref="ArgumentException">
		/// If the given <paramref name="clean_percentage"/> is greater or equal to 85.
		/// </exception>
		protected Cache(int max_size, int clean_percentage) {
			if (clean_percentage >= 85)
				throw new ArgumentException("Can't set to wipe more than 85% of the cache during clean.");

			max_cache_size = max_size;
			current_cache_size = 0;
			wipe_to = max_size - ((clean_percentage * max_size) / 100);
		}

		///<summary>
		///</summary>
		///<param name="max_size"></param>
		protected Cache(int max_size)
			: this(max_size, 20) {
		}

		///<summary>
		///</summary>
		protected Cache()
			: this(50) {
		}

		/// <summary>
		/// </summary>
		[Obsolete("Deprecated", false)]
		protected int HashSize {
			get { return (int) (max_cache_size*2) + 1; }
		}


		/// <summary>
		/// This is called whenever at object is put into the cache.
		/// </summary>
		/// <remarks>
		/// This method should determine if the cache should be cleaned and 
		/// call the clean method if appropriate.
		/// </remarks>
		protected virtual void CheckClean() {
			// If we have reached maximum cache size, remove some elements from the
			// end of the list
			if (current_cache_size >= max_cache_size) {
				Clean();
			}
		}

		/// <summary>
		/// Checks if the clean-up method should clean up more elements from 
		/// the cache.
		/// </summary>
		/// <returns>
		/// Returns <b>true</b> if the clean-up method that periodically cleans 
		/// up the cache should clean up more elements from the cache, otherwise
		/// <b>false</b>.
		/// </returns>
		protected virtual bool WipeMoreNodes() {
			return (current_cache_size >= wipe_to);
		}

		/// <summary>
		/// Notifies that the given object has been wiped from the cache by the
		/// clean up procedure.
		/// </summary>
		/// <param name="ob">The node being wiped.</param>
		protected virtual void OnWipingNode(Object ob) {
		}

		/// <summary>
		/// Notifies that the given object and key has been added to the cache.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="val"></param>
		protected void OnObjectAdded(object key, object val) {
			++current_cache_size;
		}

		/// <summary>
		/// Notifies that the given object and key has been removed from the cache.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="val"></param>
		protected void OnObjectRemoved(Object key, Object val) {
			--current_cache_size;
		}

		/// <summary>
		/// Notifies that the cache has been entirely cleared of all elements.
		/// </summary>
		protected void OnAllCleared() {
			current_cache_size = 0;
		}

		/// <summary>
		/// Notifies that some statistical information about the hash map has
		/// updated.
		/// </summary>
		/// <param name="total_walks"></param>
		/// <param name="total_get_ops"></param>
		/// <remarks>
		/// This should be used to compile statistical information about
		/// the number of walks a <i>get</i> operation takes to retreive an 
		/// entry from the hash.
		/// <para>
		/// This method is called every 8192 gets.
		/// </para>
		/// </remarks>
		protected virtual void OnGetWalks(long total_walks, long total_get_ops) {
		}


		// ---------- Public cache methods ----------

		/// <summary>
		/// Gets the number of nodes that are currently being stored in the
		/// cache.
		/// </summary>
		public virtual int NodeCount {
			get { return current_cache_size; }
		}

		protected abstract bool SetObject(object key, object value);

		protected abstract object GetObject(object key);

		protected abstract object RemoveObject(object key);

		/// <summary>
		/// Puts an object into the cache with the given key.
		/// </summary>
		/// <param name="key">The key used to store the object.</param>
		/// <param name="ob">The object to add to the cache.</param>
		public bool Set(Object key, Object ob) {

			// Do we need to clean any cache elements out?
			CheckClean();

			bool newValue = SetObject(key, ob);
			if (newValue)
				OnObjectAdded(key, ob);

			return newValue;
		}

		/// <summary>
		/// Gets the value of the node with the given key within the cache.
		/// </summary>
		/// <param name="key">The key of the node to return the value of.</param>
		/// <returns>
		/// Returns the value of the node with the given key within the cache,
		/// or <b>null</b> if none was found.
		/// </returns>
		public Object Get(Object key) {
			return GetObject(key);
		}

		/// <summary>
		/// Removes a node for the given key from the cache.
		/// </summary>
		/// <param name="key"></param>
		/// <remarks>
		/// This is useful for ensuring the cache does not contain out-dated 
		/// information.
		/// </remarks>
		/// <returns>
		/// Returns the value of the removed node or <b>null</b> if none was
		/// found for the given key.
		/// </returns>
		public Object Remove(Object key) {
			object obj = RemoveObject(key);
			
			if (obj != null)
				OnObjectRemoved(key, obj);

			return obj;
		}

		/// <summary>
		/// Clear the cache of all the entries.
		/// </summary>
		public virtual void Clear() {
			OnAllCleared();
		}


		/// <summary>
		/// Cleans away some old elements in the cache.
		/// </summary>
		/// <remarks>
		/// This method walks from the end, back <i>wipe count</i> elements 
		/// putting each object on the recycle stack.
		/// </remarks>
		/// <returns>
		/// Returns the number entries that were cleaned.
		/// </returns>
		protected virtual int Clean() {
			return 0;
		}
		
		///<summary>
		/// Gets the closest prime number to the one specified.
		///</summary>
		///<param name="value">The integer value to which the returned integer
		/// has to be close.</param>
		///<returns></returns>
		public static int ClosestPrime(int value) {
			for (int i = 0; i < PRIME_LIST.Length; ++i) {
				if (PRIME_LIST[i] >= value) {
					return PRIME_LIST[i];
				}
			}
			// Return the last prime
			return PRIME_LIST[PRIME_LIST.Length - 1];
		}

		/// <summary>
		/// A list of primes ordered from lowest to highest.
		/// </summary>
		private static readonly int[] PRIME_LIST = new int[] {
		                                                     	3001, 4799, 13999, 15377, 21803, 24247, 35083, 40531, 43669,
		                                                     	44263, 47387, 50377, 57059, 57773, 59399, 59999, 75913, 96821,
		                                                     	140551, 149011, 175633, 176389, 183299, 205507, 209771, 223099,
		                                                     	240259, 258551, 263909, 270761, 274679, 286129, 290531, 296269,
		                                                     	298021, 300961, 306407, 327493, 338851, 351037, 365489, 366811,
		                                                     	376769, 385069, 410623, 430709, 433729, 434509, 441913, 458531,
		                                                     	464351, 470531, 475207, 479629, 501703, 510709, 516017, 522211,
		                                                     	528527, 536311, 539723, 557567, 593587, 596209, 597451, 608897,
		                                                     	611069, 642547, 670511, 677827, 679051, 688477, 696743, 717683,
		                                                     	745931, 757109, 760813, 763957, 766261, 781559, 785597, 788353,
		                                                     	804493, 813559, 836917, 854257, 859973, 883217, 884789, 891493,
		                                                     	902281, 910199, 915199, 930847, 939749, 940483, 958609, 963847,
		                                                     	974887, 983849, 984299, 996211, 999217, 1007519, 1013329,
		                                                     	1014287, 1032959, 1035829, 1043593, 1046459, 1076171, 1078109,
		                                                     	1081027, 1090303, 1095613, 1098847, 1114037, 1124429, 1125017,
		                                                     	1130191, 1159393, 1170311, 1180631, 1198609, 1200809, 1212943,
		                                                     	1213087, 1226581, 1232851, 1287109, 1289867, 1297123, 1304987,
		                                                     	1318661, 1331107, 1343161, 1345471, 1377793, 1385117, 1394681,
		                                                     	1410803, 1411987, 1445261, 1460497, 1463981, 1464391, 1481173,
		                                                     	1488943, 1491547, 1492807, 1528993, 1539961, 1545001, 1548247,
		                                                     	1549843, 1551001, 1553023, 1571417, 1579099, 1600259, 1606153,
		                                                     	1606541, 1639751, 1649587, 1657661, 1662653, 1667051, 1675273,
		                                                     	1678837, 1715537, 1718489, 1726343, 1746281, 1749107, 1775489,
		                                                     	1781881, 1800157, 1806859, 1809149, 1826753, 1834607, 1846561,
		                                                     	1849241, 1851991, 1855033, 1879931, 1891133, 1893737, 1899137,
		                                                     	1909513, 1916599, 1917749, 1918549, 1919347, 1925557, 1946489,
		                                                     	1961551, 1965389, 2011073, 2033077, 2039761, 2054047, 2060171,
		                                                     	2082503, 2084107, 2095099, 2096011, 2112193, 2125601, 2144977,
		                                                     	2150831, 2157401, 2170141, 2221829, 2233019, 2269027, 2270771,
		                                                     	2292449, 2299397, 2303867, 2309891, 2312407, 2344301, 2348573,
		                                                     	2377007, 2385113, 2386661, 2390051, 2395763, 2422999, 2448367,
		                                                     	2500529, 2508203, 2509841, 2513677, 2516197, 2518151, 2518177,
		                                                     	2542091, 2547469, 2549951, 2556991, 2563601, 2575543, 2597629,
		                                                     	2599577, 2612249, 2620003, 2626363, 2626781, 2636773, 2661557,
		                                                     	2674297, 2691571, 2718269, 2725691, 2729381, 2772199, 2774953,
		                                                     	2791363, 2792939, 2804293, 2843021, 2844911, 2851313, 2863519,
		                                                     	2880797, 2891821, 2897731, 2904887, 2910251, 2928943, 2958341,
		                                                     	2975389
		                                                     };


		// ---------- Inner classes ----------

		#region Implementation of IDisposable

		public void Dispose() {
			Clear();
		}

		#endregion
	}
}