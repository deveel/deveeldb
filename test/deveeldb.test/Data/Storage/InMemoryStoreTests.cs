// 
//  Copyright 2010-2018 Deveel
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
using System.IO;

using Deveel.Data.Configurations;

using Xunit;

namespace Deveel.Data.Storage {
	public class InMemoryStoreTests : IDisposable {
		private IStoreSystem storeSystem;

		public InMemoryStoreTests() {
			storeSystem = new InMemoryStoreSystem();
		}

		[Fact]
		public void CreateAndDeleteStore() {
			storeSystem.Lock("lock1");

			var store = storeSystem.CreateStore("test", new Configuration());

			Assert.NotNull(store);
			Assert.IsType<InMemoryStore>(store);

			Assert.True(storeSystem.StoreExists("test"));

			Assert.True(storeSystem.DeleteStore(store));
			Assert.False(storeSystem.StoreExists("test"));

			Assert.Throws<IOException>(() => storeSystem.OpenStore("test", new Configuration()));

			storeSystem.Unlock("lock1");
		}

		[Fact]
		public void CreateStoreAndWriteData() {
			storeSystem.Lock("lock1");

			var store = storeSystem.CreateStore("test", new Configuration());

			var area = store.CreateArea(1024);
			Assert.NotNull(area);
			Assert.Equal(1024, area.Length);

			area.Write(12);
			area.Write(2L);
			area.Write((byte) 22);

			area.Flush();

			Assert.Equal(13, area.Position);

			Assert.True(storeSystem.CloseStore(store));

			storeSystem.Unlock("lock1");
		}

		public void Dispose() {
			storeSystem.Dispose();
		}
	}
}