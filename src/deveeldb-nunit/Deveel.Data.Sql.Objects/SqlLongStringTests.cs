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
using System.IO;
using System.Text;

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

		private ILargeObject GetLargeObject(ObjectId id) {
			return objStore.GetObject(id);
		}

		private void WriteToObject(ILargeObject obj, Encoding encoding, string text) {
			using (var stream = new ObjectStream(obj)) {
				using (var streamWriter = new StreamWriter(stream, encoding)) {
					streamWriter.Write(text);
					streamWriter.Flush();
				}
			}
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
			Assert.IsFalse(stringObj.IsNull);
		}

		[Test]
		public void WriteAndRead_Unicode() {
			const string testLine = "A simple test string that can span several characters, " +
									"that is trying to be the longest possible, just to prove" +
									"the capacity of a LONG VARCHAR to handle very long strings. " +
									"Anyway it is virtually impossible to reach the maximum size " +
									"of a long object, that is organized in 64k byte pages and " +
									"spans within the local system without any constraint of size. " +
									"For sake of memory anyway, the maximum size of the test object " +
									"is set to just 2048 bytes.";

			var obj = CreateLargeObject(2048, false);

			WriteToObject(obj, Encoding.Unicode,  testLine);

			obj.Complete();

			var objId = obj.Id;

			var stringObj = SqlLongString.Unicode(obj);
			Assert.IsNotNull(stringObj);
			Assert.IsFalse(stringObj.IsNull);

			obj = GetLargeObject(objId);
			Assert.IsTrue(obj.IsComplete);
			Assert.IsFalse(obj.IsCompressed);

			stringObj = SqlLongString.Unicode(obj);
			var reader = stringObj.GetInput(Encoding.Unicode);
			Assert.IsNotNull(reader);

			string line = null;
			Assert.DoesNotThrow(() => line = reader.ReadLine());
			Assert.IsNotNull(line);
			Assert.IsNotEmpty(line);

			Assert.AreEqual(testLine, line);
		}
	}
}
