using System;

using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	[Category("Data Objects")]
	[Category("Strings")]
	public class StringObjectTests {
		[Test]
		public void BasicVarChar_Create() {
			const string s = "Test string";
			var sObj = DataObject.VarChar(s);
			Assert.IsNotNull(sObj);
			Assert.IsInstanceOf<DataObject>(sObj);
			Assert.AreEqual(SqlTypeCode.VarChar, sObj.Type.SqlType);
			Assert.IsInstanceOf<SqlString>(sObj.Value);
			Assert.AreEqual(s, sObj.Value);
		}

		[Test]
		public void BasicVarChar_Compare() {
			const string s = "Test string";
			var sObj1 = DataObject.VarChar(s);
			var sObj2 = DataObject.VarChar(s);

			Assert.IsNotNull(sObj1);
			Assert.IsNotNull(sObj2);

			Assert.IsTrue(sObj1.IsComparableTo(sObj2));
			Assert.AreEqual(0, sObj1.CompareTo(sObj2));
		}

		[Test]
		public void BasicVarChar_Add() {
			const string s1 = "First test string that comes ";
			const string s2 = "before the second test string";
			var sObj1 = DataObject.VarChar(s1);
			var sObj2 = DataObject.VarChar(s2);

			Assert.IsNotNull(sObj1);
			Assert.IsNotNull(sObj2);

			Assert.IsTrue(sObj1.IsComparableTo(sObj2));

			DataObject result = null;
			Assert.DoesNotThrow(() => result = sObj1.Add(sObj2));
			Assert.IsNotNull(result);
			Assert.AreEqual("First test string that comes before the second test string", (string)result);
		}

		[Test]
		public void BasicVarChar_Convert_ToInteger_Success() {
			const string s = "78998";
			var obj = DataObject.VarChar(s);

			Assert.IsNotNull(obj);
			Assert.IsInstanceOf<StringType>(obj.Type);
			Assert.AreEqual(SqlTypeCode.VarChar, obj.Type.SqlType);

			DataObject result = null;
			Assert.DoesNotThrow(() => result = obj.CastTo(PrimitiveTypes.Numeric(SqlTypeCode.Integer)));
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<NumericType>(result.Type);
			Assert.AreEqual(SqlTypeCode.Integer, result.Type.SqlType);
			Assert.AreEqual(78998, result);
		}

		[Test]
		[Category("Numbers")]
		public void BasicVarChar_Convert_ToInteger_Fail() {
			const string s = "fail";
			var obj = DataObject.VarChar(s);

			Assert.IsNotNull(obj);
			Assert.IsInstanceOf<StringType>(obj.Type);
			Assert.AreEqual(SqlTypeCode.VarChar, obj.Type.SqlType);

			DataObject result = null;
			Assert.DoesNotThrow(() => result = obj.CastTo(PrimitiveTypes.Numeric(SqlTypeCode.Integer)));
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<NumericType>(result.Type);
			Assert.IsTrue(result.IsNull);
		}

		[Test]
		[Category("Booleans")]
		public void BasicVarChar_Convert_ToBoolean_Success() {
			const string s = "true";
			var obj = DataObject.VarChar(s);

			Assert.IsNotNull(obj);
			Assert.IsInstanceOf<StringType>(obj.Type);
			Assert.AreEqual(SqlTypeCode.VarChar, obj.Type.SqlType);

			DataObject result = null;
			Assert.DoesNotThrow(() => result = obj.CastTo(PrimitiveTypes.Boolean()));
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<BooleanType>(result.Type);
			Assert.IsFalse(result.IsNull);
			Assert.AreEqual(true, (bool)result);
		}
	}
}
