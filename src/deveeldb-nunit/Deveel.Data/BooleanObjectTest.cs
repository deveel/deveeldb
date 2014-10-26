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

using Deveel.Data.Sql.Objects;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public class BooleanObjectTest {
		[Test]
		public void Create_True() {
			var obj = DataObject.Boolean(true);
			Assert.IsNotNull(obj);
			Assert.IsInstanceOf<SqlBoolean>(obj.Value);
			Assert.AreEqual(true, (bool)obj.AsBoolean());
		}

		[Test]
		public void Convert_True_ToNumber() {
			var obj = DataObject.Boolean(true);
			Assert.IsNotNull(obj);
			Assert.IsInstanceOf<SqlBoolean>(obj.Value);
			Assert.AreEqual(true, (bool)obj.AsBoolean());

			DataObject numObj = null;
			Assert.DoesNotThrow(() => numObj = obj.AsInteger());
			Assert.IsNotNull(numObj);
			Assert.AreEqual(1, (int) numObj);
		}

		[Test]
		public void Create_False() {
			var obj = DataObject.Boolean(false);
			Assert.IsNotNull(obj);
			Assert.IsInstanceOf<SqlBoolean>(obj.Value);
			Assert.AreEqual(false, (bool)obj.AsBoolean());
		}

		[Test]
		public void Convert_False_ToNumber() {
			var obj = DataObject.Boolean(false);
			Assert.IsNotNull(obj);
			Assert.IsInstanceOf<SqlBoolean>(obj.Value);
			Assert.AreEqual(false, (bool)obj.AsBoolean());

			DataObject numObj = null;
			Assert.DoesNotThrow(() => numObj = obj.AsInteger());
			Assert.IsNotNull(numObj);
			Assert.AreEqual(0, (int) numObj);
		}

		[Test]
		public void Create_Null() {
			var obj = DataObject.BooleanNull;
			Assert.IsNotNull(obj);
			Assert.IsInstanceOf<SqlBoolean>(obj.Value);
			Assert.IsTrue(obj.IsNull);
			Assert.AreEqual(SqlNull.Value, obj.AsBoolean().Value);
		}

		[Test]
		public void Convert_Null_ToNumber() {
			var obj = DataObject.BooleanNull;
			Assert.IsNotNull(obj);
			Assert.IsInstanceOf<SqlBoolean>(obj.Value);
			Assert.AreEqual(SqlNull.Value, obj.AsBoolean().Value);

			DataObject numObj = null;
			Assert.DoesNotThrow(() => numObj = obj.AsInteger());
			Assert.IsNotNull(numObj);
			Assert.IsTrue(numObj.IsNull);
			Assert.AreEqual(SqlNull.Value, numObj.Value);
		}
	}
}
