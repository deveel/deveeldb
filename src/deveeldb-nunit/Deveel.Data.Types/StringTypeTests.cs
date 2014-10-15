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

namespace Deveel.Data.Types {
	[TestFixture]
	[Category("Data Types")]
	public class StringTypeTests {
		[Test]
		public void BasicVarChar_Create() {
			var type = PrimitiveTypes.String(SqlTypeCode.VarChar);
			Assert.AreEqual(SqlTypeCode.VarChar, type.SqlType);
			Assert.AreEqual(Int16.MaxValue, type.MaxSize);
			Assert.IsTrue(type.IsPrimitive);
			Assert.IsTrue(type.IsIndexable);
			Assert.IsNull(type.Locale);
		}

		[Test]
		public void BasicVarChar_Compare() {
			var type1 = PrimitiveTypes.String(SqlTypeCode.VarChar);
			var type2 = PrimitiveTypes.String(SqlTypeCode.VarChar);

			Assert.AreEqual(type1.SqlType, type2.SqlType);
			Assert.IsTrue(type1.IsComparable(type2));
			Assert.IsTrue(type1.CanCastTo(type2));
		}

		[Test]
		public void BasicVarChar_Parse() {
			const string typeString = "VARCHAR";
			DataType dataType = null;
			Assert.DoesNotThrow(() => dataType = DataType.Parse(typeString));
			Assert.IsNotNull(dataType);
			Assert.IsInstanceOf<StringType>(dataType);
			Assert.AreEqual(SqlTypeCode.VarChar, dataType.SqlType);

			var stringType = (StringType) dataType;
			Assert.AreEqual(Int16.MaxValue, stringType.MaxSize);
			Assert.AreEqual(null, stringType.Locale);
		}

		[Test]
		public void SizedVarChar_Create() {
			var type = PrimitiveTypes.String(SqlTypeCode.VarChar, 255);
			Assert.AreEqual(SqlTypeCode.VarChar, type.SqlType);
			Assert.AreEqual(255, type.MaxSize);
		}

		[Test]
		public void SizedVarChar_Compare() {
			var type1 = PrimitiveTypes.String(SqlTypeCode.VarChar, 255);
			var type2 = PrimitiveTypes.String(SqlTypeCode.VarChar, 200);

			Assert.AreEqual(type1.SqlType, type2.SqlType);
			Assert.IsFalse(type1.Equals(type2));
			Assert.IsTrue(type1.IsComparable(type2));
		}

		[Test]
		public void SizedVarChar_Parse() {
			const string typeString = "VARCHAR(255)";
			DataType dataType = null;
			Assert.DoesNotThrow(() => dataType = DataType.Parse(typeString));
			Assert.IsNotNull(dataType);
			Assert.IsInstanceOf<StringType>(dataType);
			Assert.AreEqual(SqlTypeCode.VarChar, dataType.SqlType);

			var stringType = (StringType) dataType;
			Assert.AreEqual(255, stringType.MaxSize);
			Assert.AreEqual(null, stringType.Locale);			
		}

		[Test]
		public void LocalizedVarChar_Parse() {
			const string typeString = "VARCHAR(255) LOCALE 'en-Us'";
						DataType dataType = null;
			Assert.DoesNotThrow(() => dataType = DataType.Parse(typeString));
			Assert.IsNotNull(dataType);
			Assert.IsInstanceOf<StringType>(dataType);
			Assert.AreEqual(SqlTypeCode.VarChar, dataType.SqlType);

			var stringType = (StringType) dataType;
			Assert.AreEqual(255, stringType.MaxSize);
			Assert.AreEqual(null, stringType.Locale);
		}
	}
}