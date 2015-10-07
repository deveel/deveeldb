using System;

namespace Deveel.Data.Caching {
	public class SizeLimitedCache : MemoryCache {
		public SizeLimitedCache(int maxSize) {
			MaxSize = maxSize;
		}

		public int MaxSize { get; private set; }

		protected override void UpdateElementAccess(object key, CacheValue cacheValue) {
			base.UpdateElementAccess(key, cacheValue);

			while (IndexList.Count > MaxSize) {
				RemoveUnlocked(IndexList.Last.Value.Key);
			}
		}
	}
}
