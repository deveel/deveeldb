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

namespace Deveel.Data.Sql.Types {
	[TestFixture]
	public sealed class DataTypeParseTests : ContextBasedTest {
		[Test]
		public void SimpleNumeric() {
			const string typeString = "NUMERIC";

			SqlType type = null;
			Assert.DoesNotThrow(() => type = SqlType.Parse(typeString));
			Assert.IsNotNull(type);
			Assert.IsInstanceOf<NumericType>(type);
			Assert.AreEqual(-1, ((NumericType)type).Precision);
		}

		[TestCase("BOOLEAN")]
		[TestCase("boolean")]
		[TestCase("BIT")]
		public void Boolean(string typeString) {
			SqlType type = null;
			Assert.DoesNotThrow(() => type = SqlType.Parse(typeString));
			Assert.IsNotNull(type);
			Assert.IsInstanceOf<BooleanType>(type);
		}

		[TestCase("app.test%ROWTYPE", "app.test")]
		[TestCase("test%ROWTYPE", "test")]
		public void RowType(string typeString, string expectedName) {
			var type = SqlType.Parse(typeString);
			Assert.IsNotNull(type);
			Assert.IsInstanceOf<RowRefType>(type);
			Assert.IsTrue(type.IsReference);

			var objName = ((RowRefType) type).ObjectName;
			Assert.IsNotNull(objName);
			Assert.AreEqual(expectedName, objName.FullName);
		}

		[TestCase("APP.test_table.col1%TYPE", "APP.test_table.col1")]
		[TestCase("var1%TYPE", "var1")]
		public void FieldType(string typeString, string expectedName) {
			var type = SqlType.Parse(typeString);
			Assert.IsNotNull(type);
			Assert.IsInstanceOf<FieldRefType>(type);
			Assert.IsTrue(type.IsReference);

			var fieldName = ((FieldRefType)type).FieldName;
			Assert.IsNotNull(fieldName);
			Assert.AreEqual(expectedName, fieldName.FullName);
		}
	}
}
