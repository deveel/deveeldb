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
	/// The <see cref="ICache"/> implementation built on an instance of
	/// <see cref="ICache"/> that is wrapped in.
	/// </summary>
	/// <remarks>
	/// This class is generally used to wrap a configured cache system.
	/// </remarks>
	internal class CacheWrapper : Cache {
		protected CacheWrapper(ICache cache, int max_size)
			: base(max_size) {
			this.cache = cache;
		}

		private readonly ICache cache;

		#region Overrides of Cache

		protected override bool SetObject(object key, object value) {
			return cache.Set(key, value);
		}

		protected override object GetObject(object key) {
			return cache.Get(key);
		}

		protected override object RemoveObject(object key) {
			return cache.Remove(key);
		}

		#endregion

		public override void Clear() {
			cache.Clear();
			base.Clear();
		}
	}
}