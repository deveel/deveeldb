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
using System.Collections.Generic;

using Deveel.Data.Configuration;

namespace Deveel.Data.Caching {
	public class MemoryCache : Cache {
		protected LinkedList<KeyValuePair<object, CacheValue>> IndexList { get; private set; }

		private readonly Dictionary<object, CacheValue> valueCache = new Dictionary<object, CacheValue>();

		private readonly object syncRoot = new object();
		private DateTime lastCacheAccess = DateTime.MaxValue;

		public MemoryCache() {
			IndexList = new LinkedList<KeyValuePair<object, CacheValue>>();
		}

		// Some statistics about the hashing algorithm.
		private long totalGets = 0;
		private long getTotal = 0;

		protected virtual void UpdateElementAccess(object key, CacheValue cacheValue) {
			// update last access and move it to the head of the list
			cacheValue.LastAccess = DateTime.Now;
			var idxRef = cacheValue.IndexRef;
			if (idxRef != null) {
				IndexList.Remove(idxRef);
			} else {
				idxRef = new LinkedListNode<KeyValuePair<object, CacheValue>>(new KeyValuePair<object, CacheValue>(key, cacheValue));
				cacheValue.IndexRef = idxRef;
			}

			IndexList.AddFirst(idxRef);
		}

		protected virtual CacheValue GetCacheValueUnlocked(object key) {
			CacheValue v;
			return valueCache.TryGetValue(key, out v) ? v : null;
		}

		protected virtual CacheValue SetValueUnlocked(object key, object value) {
			lastCacheAccess = DateTime.Now;
			var cacheValue = GetCacheValueUnlocked(key);

			if (cacheValue == null) {
				cacheValue = new CacheValue(value);
				cacheValue.IsNew = true;
				valueCache[key] = cacheValue;
			} else {
				cacheValue.Value = value;
				cacheValue.IsNew = false;
			}

			UpdateElementAccess(key, cacheValue);
			return cacheValue;
		}

		protected object RemoveUnlocked(object key) {
			var value = GetCacheValueUnlocked(key);
			if (value != null) {
				valueCache.Remove(key);
				IndexList.Remove(value.IndexRef);
				return value.Value;
			}

			return null;
		}

		protected override void ConfigureCache(IConfiguration config) {
			// TODO:
			base.ConfigureCache(config);
		}

		protected override bool SetObject(object key, object value) {
			lock (syncRoot) {
				var cached = SetValueUnlocked(key, value);
				return cached.IsNew;
			}
		}

		protected override bool TryGetObject(object key, out object value) {
			CacheValue v;
			value = null;

			lock (syncRoot) {
				lastCacheAccess = DateTime.Now;
				v = GetCacheValueUnlocked(key);
				if (v != null) {
					value = v.Value;
					UpdateElementAccess(key, v);
					return true;
				}
			}

			return false;
		}

		protected override object RemoveObject(object key) {
			lock (syncRoot) {
				lastCacheAccess = DateTime.Now;

				return RemoveUnlocked(key);
			}
		}

		public override void Clear() {
			lock (syncRoot) {
				valueCache.Clear();
				IndexList.Clear();
			}

			base.Clear();
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				Clear();
			}


			base.Dispose(disposing);
		}

		#region CachedValue

		protected sealed class CacheValue {
			public CacheValue(object value) {
				LastAccess = DateTime.Now;
				Value = value;
			}

			public LinkedListNode<KeyValuePair<object, CacheValue>> IndexRef { get; set; }

			public DateTime LastAccess { get; set; }

			public object Value { get; set; }

			public bool IsNew { get; set; }
		}

		#endregion
	}
}