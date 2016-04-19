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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	[Category("Data Objects")]
	[Category("Strings")]
	public class StringObjectTests {
		[Test]
		public void BasicVarChar_Create() {
			const string s = "Test string";
			var sObj = Field.VarChar(s);
			Assert.IsNotNull(sObj);
			Assert.IsInstanceOf<Field>(sObj);
			Assert.AreEqual(SqlTypeCode.VarChar, sObj.Type.TypeCode);
			Assert.IsInstanceOf<SqlString>(sObj.Value);
			Assert.AreEqual(s, sObj.Value);
		}

		[Test]
		public void BasicVarChar_Compare() {
			const string s = "Test string";
			var sObj1 = Field.VarChar(s);
			var sObj2 = Field.VarChar(s);

			Assert.IsNotNull(sObj1);
			Assert.IsNotNull(sObj2);

			Assert.IsTrue(sObj1.IsComparableTo(sObj2));
			Assert.AreEqual(0, sObj1.CompareTo(sObj2));
		}

		[Test]
		public void BasicVarChar_Add() {
			const string s1 = "First test string that comes ";
			const string s2 = "before the second test string";
			var sObj1 = Field.VarChar(s1);
			var sObj2 = Field.VarChar(s2);

			Assert.IsNotNull(sObj1);
			Assert.IsNotNull(sObj2);

			Assert.IsTrue(sObj1.IsComparableTo(sObj2));

			Field result = null;
			Assert.DoesNotThrow(() => result = sObj1.Add(sObj2));
			Assert.IsNotNull(result);
			Assert.AreEqual("First test string that comes before the second test string", (string)result);
		}

		[Test]
		public void BasicVarChar_Convert_ToInteger_Success() {
			const string s = "78998";
			var obj = Field.VarChar(s);

			Assert.IsNotNull(obj);
			Assert.IsInstanceOf<StringType>(obj.Type);
			Assert.AreEqual(SqlTypeCode.VarChar, obj.Type.TypeCode);

			Field result = null;
			Assert.DoesNotThrow(() => result = obj.CastTo(PrimitiveTypes.Numeric(SqlTypeCode.Integer)));
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<NumericType>(result.Type);
			Assert.AreEqual(SqlTypeCode.Integer, result.Type.TypeCode);
			Assert.AreEqual(78998, result);
		}

		[Test]
		[Category("Numbers")]
		public void BasicVarChar_Convert_ToInteger_Fail() {
			const string s = "fail";
			var obj = Field.VarChar(s);

			Assert.IsNotNull(obj);
			Assert.IsInstanceOf<StringType>(obj.Type);
			Assert.AreEqual(SqlTypeCode.VarChar, obj.Type.TypeCode);

			Field result = null;
			Assert.DoesNotThrow(() => result = obj.CastTo(PrimitiveTypes.Numeric(SqlTypeCode.Integer)));
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<NumericType>(result.Type);
			Assert.IsTrue(result.IsNull);
		}

		[Test]
		[Category("Booleans")]
		public void BasicVarChar_Convert_ToBoolean_Success() {
			const string s = "true";
			var obj = Field.VarChar(s);

			Assert.IsNotNull(obj);
			Assert.IsInstanceOf<StringType>(obj.Type);
			Assert.AreEqual(SqlTypeCode.VarChar, obj.Type.TypeCode);

			Field result = null;
			Assert.DoesNotThrow(() => result = obj.CastTo(PrimitiveTypes.Boolean()));
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<BooleanType>(result.Type);
			Assert.IsFalse(result.IsNull);
			Assert.AreEqual(true, (bool)result);
		}
	}
}
