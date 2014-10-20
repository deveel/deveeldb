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
	[Category("Booleans")]
	public class SqlBooleanTest {
		[Test]
		public void CreateFromByte() {
			var value = new SqlBoolean(1);
			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.AreEqual(true, (bool)value);

			value = new SqlBoolean(0);
			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.AreEqual(false, (bool)value);
		}

		[Test]
		public void CreateFromBoolean() {
			var value = new SqlBoolean(true);
			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.AreEqual(true, (bool)value);

			value = new SqlBoolean(false);
			Assert.IsNotNull(value);
			Assert.IsFalse(value.IsNull);
			Assert.AreEqual(false, (bool)value);
		}

		[Test]
		[Category("Comparison")]
		public void Compare_Equal() {
			var value1 = SqlBoolean.True;
			var value2 = new SqlBoolean(true);

			Assert.IsFalse(value1.IsNull);
			Assert.IsFalse(value2.IsNull);

			Assert.IsTrue(value1.IsComparableTo(value2));

			int i = -2;
			Assert.DoesNotThrow(() => i = value1.CompareTo(value2));
			Assert.AreEqual(0, i);
		}

		[Test]
		[Category("Comparison")]
		public void Compare_NotEqual() {
			var value1 = SqlBoolean.False;
			var value2 = new SqlBoolean(true);

			Assert.IsFalse(value1.IsNull);
			Assert.IsFalse(value2.IsNull);

			Assert.IsTrue(value1.IsComparableTo(value2));

			int i = -2;
			Assert.DoesNotThrow(() => i = value1.CompareTo(value2));
			Assert.AreEqual(-1, i);
		}

		[Test]
		[Category("Comparison")]
		public void Compare_ToBooleanNull() {
			var value1 = SqlBoolean.True;
			var value2 = SqlBoolean.Null;

			Assert.IsFalse(value1.IsNull);
			Assert.IsTrue(value2.IsNull);

			Assert.IsTrue(value1.IsComparableTo(value2));

			int i = -2;
			Assert.DoesNotThrow(() => i = value1.CompareTo(value2));
			Assert.AreEqual(1, i);
		}

		[Test]
		[Category("Comparison")]
		public void Compare_ToNull() {
			var value1 = SqlBoolean.True;
			var value2 = SqlNull.Value;

			Assert.IsFalse(value1.IsNull);
			Assert.IsTrue(value2.IsNull);

			Assert.IsTrue(value1.IsComparableTo(value2));

			int i = -2;
			Assert.DoesNotThrow(() => i = value1.CompareTo(value2));
			Assert.AreEqual(1, i);			
		}

		[Test]
		[Category("Comparison")]
		[Category("Numbers")]
		public void Compare_ToNumber_InRange() {
			var value1 = SqlBoolean.True;
			var value2 = SqlNumber.One;

			Assert.IsFalse(value1.IsNull);
			Assert.IsFalse(value2.IsNull);

			Assert.IsTrue(value1.IsComparableTo(value2));

			int i = -2;
			Assert.DoesNotThrow(() => i = value1.CompareTo(value2));
			Assert.AreEqual(0, i);

			value2 = SqlNumber.Zero;

			Assert.IsFalse(value1.IsNull);
			Assert.IsFalse(value2.IsNull);

			Assert.IsTrue(value1.IsComparableTo(value2));

			i = -2;
			Assert.DoesNotThrow(() => i = value1.CompareTo(value2));
			Assert.AreEqual(1, i);
		}

		[Test]
		[Category("Comparison")]
		[Category("Numbers")]
		public void Compare_ToNumber_OutOfRange() {
			var value1 = SqlBoolean.True;
			var value2 = new SqlNumber(21);

			Assert.IsFalse(value1.IsNull);
			Assert.IsFalse(value2.IsNull);

			Assert.IsFalse(value1.IsComparableTo(value2));

			int i = -2;
			Assert.Throws<ArgumentOutOfRangeException>(() => i = value1.CompareTo(value2));
			Assert.AreEqual(-2, i);
		}

		[Test]
		[Category("Operators")]
		public void Equality_True() {
			var value1 = SqlBoolean.True;
			var value2 = SqlBoolean.True;

			Assert.IsTrue(value1 == value2);
		}

		[Test]
		[Category("Operators")]
		public void Equality_False() {
			var value1 = SqlBoolean.True;
			var value2 = SqlBoolean.False;
			
			Assert.IsTrue(value1 != value2);

			value2 = SqlBoolean.Null;

			Assert.IsTrue(value1 != value2);
		}

		[Test]
		[Category("Operators")]
		public void Equality_ToNull_True() {
			var value1 = SqlBoolean.Null;
			var value2 = SqlNull.Value;

			Assert.IsTrue(value1 == value2);
		}

		[Test]
		[Category("Conversion")]
		public void Convert_ToString() {
			var value = SqlBoolean.True;
			Assert.AreEqual("true", value.ToString());

			value = SqlBoolean.False;
			Assert.AreEqual("false", value.ToString());

			value = SqlBoolean.Null;
			Assert.AreEqual("NULL", value.ToString());
		}
	}
}
