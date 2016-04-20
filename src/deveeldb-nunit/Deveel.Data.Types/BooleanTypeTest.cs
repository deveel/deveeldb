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

namespace Deveel.Data.Sql.Types {
	[TestFixture]
	[Category("Booleans")]
	[Category("Data Types")]
	public class BooleanTypeTest {
		[Test]
		[Category("Comparison")]
		public void Compare_Booleans() {
			var type = PrimitiveTypes.Boolean();
			Assert.IsNotNull(type);

			Assert.AreEqual(1, type.Compare(SqlBoolean.True, SqlBoolean.False));
			Assert.AreEqual(-1, type.Compare(SqlBoolean.False, SqlBoolean.True));
			Assert.AreEqual(0, type.Compare(SqlBoolean.True, SqlBoolean.True));
			Assert.AreEqual(0, type.Compare(SqlBoolean.False, SqlBoolean.False));
		}

		[Test]
		[Category("Numbers")]
		[Category("Comparison")]
		public void Compare_BooleanToNumeric() {
			var type = PrimitiveTypes.Boolean();
			Assert.IsNotNull(type);

			Assert.AreEqual(0, type.Compare(SqlBoolean.True, SqlNumber.One));
			Assert.AreEqual(0, type.Compare(SqlBoolean.False, SqlNumber.Zero));
		}

		[Test]
		[Category("Numbers")]
		[Category("Comparison")]
		public void Compare_BooleanToNumeric_Invalid() {
			var type = PrimitiveTypes.Boolean();
			Assert.IsNotNull(type);

			int result = -2;
			Assert.DoesNotThrow(() => result = type.Compare(SqlBoolean.True, new SqlNumber(22)));
			Assert.AreEqual(1, result);
		}

		[TestCase(SqlTypeCode.Bit, true, "1")]
		[TestCase(SqlTypeCode.Bit, false, "0")]
		[TestCase(SqlTypeCode.Boolean, true, "true")]
		[TestCase(SqlTypeCode.Boolean, false, "false")]
		public void CastToString(SqlTypeCode typeCode, bool value, string expected) {
			var type = PrimitiveTypes.Boolean(typeCode);

			var boolean = new SqlBoolean(value);

			var casted = type.CastTo(boolean, PrimitiveTypes.String());

			Assert.IsInstanceOf<SqlString>(casted);
			Assert.AreEqual(expected, casted.ToString());
		}

		[TestCase(true, 1)]
		[TestCase(false, 0)]
		public void CastToNumber(bool value, int expected) {
			var type = PrimitiveTypes.Boolean();
			var boolean = new SqlBoolean(value);

			var casted = type.CastTo(boolean, PrimitiveTypes.Numeric());

			Assert.IsInstanceOf<SqlNumber>(casted);
			Assert.AreEqual(expected, ((SqlNumber)casted).ToInt32());
		}

		[TestCase(true, 1)]
		[TestCase(false, 0)]
		public void CastToBinary(bool value, byte expected) {
			var type = PrimitiveTypes.Boolean();
			var boolean = new SqlBoolean(value);

			var casted = type.CastTo(boolean, PrimitiveTypes.Binary());

			var expectedArray = new[] {expected};

			Assert.IsInstanceOf<SqlBinary>(casted);
			Assert.AreEqual(expectedArray, ((SqlBinary)casted).ToByteArray());
		}
	}
}
