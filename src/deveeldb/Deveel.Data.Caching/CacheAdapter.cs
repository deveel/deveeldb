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

namespace Deveel.Data.Caching {
	class CacheAdapter : Cache {
		public CacheAdapter(ICache baseCache) {
			BaseCache = baseCache;
		}

		protected ICache BaseCache { get; private set; }

		protected override bool SetObject(object key, object value) {
			return BaseCache.Set(key, value);
		}

		protected override bool TryGetObject(object key, out object value) {
			return BaseCache.TryGet(key, out value);
		}

		protected override object RemoveObject(object key) {
			return BaseCache.Remove(key);
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (BaseCache != null)
					BaseCache.Clear();
			}

			BaseCache = null;
			base.Dispose(disposing);
		}

		public override void Clear() {
			if (BaseCache != null)
				BaseCache.Clear();

			base.Clear();
		}
	}
}
