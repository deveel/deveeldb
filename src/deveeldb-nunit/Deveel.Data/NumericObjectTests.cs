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

using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	[Category("Data Objects")]
	[Category("Numbers")]
	public class NumericObjectTests {
		[Test]
		public void Integer_Create() {
			var obj = DataObject.Integer(33);
			Assert.IsNotNull(obj);
			Assert.IsInstanceOf<NumericType>(obj.Type);
			Assert.AreEqual(SqlTypeCode.Integer, obj.Type.SqlType);
			Assert.AreEqual(33, obj);
		}

		[Test]
		public void BigInt_Create() {
			var obj = DataObject.BigInt(8399902L);
			Assert.IsNotNull(obj);
			Assert.IsInstanceOf<NumericType>(obj.Type);
			Assert.AreEqual(SqlTypeCode.BigInt, obj.Type.SqlType);
			Assert.AreEqual(8399902L, obj);			
		}

		[Test]
		public void Integer_Compare_Equal() {
			var obj1 = DataObject.Integer(33);
			var obj2 = DataObject.Integer(33);

			Assert.IsNotNull(obj1);
			Assert.IsNotNull(obj2);

			Assert.IsInstanceOf<NumericType>(obj1.Type);
			Assert.IsInstanceOf<NumericType>(obj2.Type);

			Assert.AreEqual(SqlTypeCode.Integer, obj1.Type.SqlType);
			Assert.AreEqual(SqlTypeCode.Integer, obj2.Type.SqlType);

			Assert.IsTrue(obj1.IsComparableTo(obj2));
			Assert.AreEqual(0, obj1.CompareTo(obj2));
		}

		[Test]
		public void Integer_Compare_NotEqual() {
			var obj1 = DataObject.Integer(33);
			var obj2 = DataObject.Integer(87);

			Assert.IsNotNull(obj1);
			Assert.IsNotNull(obj2);

			Assert.IsInstanceOf<NumericType>(obj1.Type);
			Assert.IsInstanceOf<NumericType>(obj2.Type);

			Assert.AreEqual(SqlTypeCode.Integer, obj1.Type.SqlType);
			Assert.AreEqual(SqlTypeCode.Integer, obj2.Type.SqlType);

			Assert.IsTrue(obj1.IsComparableTo(obj2));
			Assert.AreEqual(-1, obj1.CompareTo(obj2));
		}
	}
}