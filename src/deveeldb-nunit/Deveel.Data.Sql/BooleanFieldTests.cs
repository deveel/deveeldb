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
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public static class BooleanFieldTests {
		[Test]
		public static void ToInteger_True() {
			var value = Field.BooleanTrue;
			var i = value.AsInteger();

			Assert.IsNotNull(i);
			Assert.IsFalse(i.IsNull);
			Assert.IsInstanceOf<NumericType>(i.Type);
			Assert.IsInstanceOf<SqlNumber>(i.Value);

			var v = (SqlNumber) i.Value;

			Assert.IsTrue(v.CanBeInt32);
			Assert.AreEqual(1, v.ToInt32());
			Assert.AreEqual(true, v.ToBoolean());
		}

		[Test]
		public static void ToInteger_False() {
			var value = Field.BooleanFalse;
			var i = value.AsInteger();

			Assert.IsNotNull(i);
			Assert.IsFalse(i.IsNull);
			Assert.IsInstanceOf<NumericType>(i.Type);
			Assert.IsInstanceOf<SqlNumber>(i.Value);

			var v = (SqlNumber)i.Value;

			Assert.IsTrue(v.CanBeInt32);
			Assert.AreEqual(0, v.ToInt32());
			Assert.AreEqual(false, v.ToBoolean());
		}

		[Test]
		public static void ToVarChar_True() {
			var value = Field.BooleanTrue;
			var v = value.AsVarChar();

			Assert.IsNotNull(v);
			Assert.IsInstanceOf<StringType>(v.Type);
			Assert.AreEqual(SqlTypeCode.VarChar, v.Type.TypeCode);
			Assert.IsInstanceOf<SqlString>(v.Value);

			var s = (SqlString) v.Value;

			Assert.AreEqual("True", s.Value);
		}

		[Test]
		public static void ToVarChar_False() {
			var value = Field.BooleanTrue;
			var v = value.AsVarChar();

			Assert.IsNotNull(v);
			Assert.IsInstanceOf<StringType>(v.Type);
			Assert.AreEqual(SqlTypeCode.VarChar, v.Type.TypeCode);
			Assert.IsInstanceOf<SqlString>(v.Value);

			var s = (SqlString)v.Value;

			Assert.AreEqual("True", s.Value);
		}

		[Test]
		public static void ToUnsupported() {
			var value = Field.BooleanTrue;
			Field v = null;

			// CHECK: Not sure if casting to an invalid destination should
			//        throw or return a null value

			Assert.Throws<InvalidCastException>(() => v = value.AsTimeStamp());
			
			//Assert.IsNotNull(v);
			//Assert.IsInstanceOf<DateType>(v.Type);
			//Assert.AreEqual(SqlTypeCode.TimeStamp, v.Type.TypeCode);
			//Assert.IsInstanceOf<SqlDateTime>(v.Value);
			//Assert.IsTrue(v.IsNull);
			//Assert.IsTrue(v.Value.IsNull);
		}

		[Test]
		public static void Or() {
			var v1 = Field.BooleanTrue;
			var v2 = Field.BooleanFalse;

			var result = v1.Or(v2);

			Assert.IsNotNull(result);
			Assert.IsInstanceOf<BooleanType>(result.Type);
			Assert.IsInstanceOf<SqlBoolean>(result.Value);

			Assert.AreEqual(SqlBoolean.True, result.Value);
		}
	}
}
