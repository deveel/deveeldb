// 
//  Copyright 2010-2014 Deveel
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

using NUnit.Framework;

namespace Deveel.Data.Caching {
	[TestFixture]
	public class MemoryCacheTests {
		[Test]
		public void CreateWithoutErrors() {
			MemoryCache cache = null;
			Assert.DoesNotThrow(() => cache = new MemoryCache());
			Assert.IsNotNull(cache);
		}

		[Test]
		public void SetValue() {
			var cache = new MemoryCache();

			bool result = false;
			Assert.DoesNotThrow(() => result = cache.Set(32, "test"));
			Assert.IsTrue(result);
		}

		[Test]
		public void SetAndGetValue() {
			var cache = new MemoryCache();
			Assert.IsTrue(cache.Set(32, "test"));

			object value = null;
			Assert.DoesNotThrow(() => {
				bool found = cache.TryGet(32, out value);
				Assert.IsTrue(found);
			});

			Assert.IsNotNull(value);
			Assert.AreEqual("test", value);
		}
	}
}