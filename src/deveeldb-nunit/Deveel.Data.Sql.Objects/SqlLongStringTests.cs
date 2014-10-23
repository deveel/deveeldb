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

using Deveel.Data.Store;

using NUnit.Framework;

namespace Deveel.Data.Sql.Objects {
	[TestFixture]
	public class SqlLongStringTests {
		private InMemoryStorageSystem storageSystem;
		private InMemoryStore testStore;
		private ObjectStore objStore;

		private ILargeObject CreateLargeObject(long size, bool compressed) {
			return objStore.CreateNewObject(size, compressed);
		}

		[SetUp]
		public void TestSetUp() {
			storageSystem = new InMemoryStorageSystem();
			testStore = storageSystem.CreateStore("TestStore");
			objStore = new ObjectStore(1, testStore);
			objStore.Create();
		}

		[TearDown]
		public void TestTearDown() {
			storageSystem.DeleteStore(testStore);
			storageSystem.Dispose();
			storageSystem = null;
		}

		[Test]
		public void Create_Unicode_Uncompressed() {
			var obj = CreateLargeObject(2048, false);
			var stringObj = SqlLongString.Unicode(obj);
			Assert.IsNotNull(stringObj);
		}
	}
}
