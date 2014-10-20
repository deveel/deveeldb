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

namespace Deveel.Data.Types {
	[TestFixture]
	public class BooleanTypeTest {
		[Test]
		public void Compare_Booleans() {
			var type = PrimitiveTypes.Boolean();
			Assert.IsNotNull(type);

			Assert.AreEqual(1, type.Compare(SqlBoolean.True, SqlBoolean.False));
			Assert.AreEqual(-1, type.Compare(SqlBoolean.False, SqlBoolean.True));
			Assert.AreEqual(0, type.Compare(SqlBoolean.True, SqlBoolean.True));
			Assert.AreEqual(0, type.Compare(SqlBoolean.False, SqlBoolean.False));
		}

		[Test]
		public void Compare_BooleanToNumeric() {
			var type = PrimitiveTypes.Boolean();
			Assert.IsNotNull(type);

			Assert.AreEqual(0, type.Compare(SqlBoolean.True, SqlNumber.One));
			Assert.AreEqual(0, type.Compare(SqlBoolean.False, SqlNumber.Zero));
		}
	}
}
