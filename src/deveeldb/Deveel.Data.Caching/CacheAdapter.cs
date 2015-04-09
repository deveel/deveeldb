using System;

namespace Deveel.Data.Caching {
	class CacheAdapter : Cache {
		public CacheAdapter(ICache baseCache, int maxSize)
			: base(maxSize) {
			BaseCache = baseCache;
		}

		protected ICache BaseCache { get; private set; }

		protected override bool SetObject(object key, object value) {
			return BaseCache.Set(key, value);
		}

		protected override object GetObject(object key) {
			return BaseCache.Get(key);
		}

		protected override object RemoveObject(object key) {
			return BaseCache.Remove(key);
		}

		public override void Clear() {
			BaseCache.Clear();
			base.Clear();
		}
	}
}
