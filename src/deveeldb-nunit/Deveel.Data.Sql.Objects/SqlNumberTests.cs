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

namespace Deveel.Data.Sql.Objects {
	[TestFixture]
	[Category("SQL Objects")]
	[Category("Numbers")]
	public class SqlNumberTests {
		[Test]
		public void Create_FromInteger() {
			var value = new SqlNumber((int) 45993);
			Assert.IsFalse(value.IsNull);
			Assert.IsTrue(value.CanBeInt32);
			Assert.IsTrue(value.CanBeInt64);
			Assert.AreEqual(0, value.Scale);
			Assert.AreEqual(5, value.Precision);
			Assert.AreEqual(NumericState.None, value.State);
			Assert.AreEqual(1, value.Sign);
		}

		[Test]
		public void Create_FromBigInt() {
			var value = new SqlNumber(4599356655L);
			Assert.IsFalse(value.IsNull);
			Assert.IsFalse(value.CanBeInt32);
			Assert.IsTrue(value.CanBeInt64);
			Assert.AreEqual(0, value.Scale);
			Assert.AreEqual(10, value.Precision);
			Assert.AreEqual(NumericState.None, value.State);
			Assert.AreEqual(1, value.Sign);			
		}

		[Test]
		public void Create_FromDouble() {
			var value = new SqlNumber(459935.9803d);
			Assert.IsFalse(value.IsNull);
			Assert.IsFalse(value.CanBeInt32);
			Assert.IsFalse(value.CanBeInt64);
			Assert.AreEqual(28, value.Scale);
			Assert.AreEqual(34, value.Precision);
			Assert.AreEqual(NumericState.None, value.State);
			Assert.AreEqual(1, value.Sign);
		}

		[Test]
		public void Parse_BigDecimal() {
			var value = new SqlNumber();
			Assert.DoesNotThrow(() => value = SqlNumber.Parse("98356278.911288837773848500069994933229238e45789"));
			Assert.IsFalse(value.IsNull);
			Assert.IsFalse(value.CanBeInt32);
			Assert.IsFalse(value.CanBeInt64);
			Assert.Greater(value.Precision, 40);
		}

		[Test]
		public void Convert_ToBoolean_Success() {
			var value = SqlNumber.One;
			var b = new SqlBoolean();
			Assert.DoesNotThrow(() => b = (SqlBoolean)Convert.ChangeType(value, typeof(SqlBoolean)));
			Assert.IsTrue(b);
		}
	}
}