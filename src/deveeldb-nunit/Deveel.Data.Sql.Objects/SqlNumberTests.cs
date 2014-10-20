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
			
		}

		[Test]
		public void Parse_BigDecimal() {
			
		}
	}
}