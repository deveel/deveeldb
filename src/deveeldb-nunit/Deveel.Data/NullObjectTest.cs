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
using Deveel.Data.Sql.Types;

using NUnit;
using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public class NullObjectTest {
		[Test]
		public void NullObjectEqualsDbNull() {
			Field obj = null;
			Assert.AreEqual(DBNull.Value, (DBNull)obj);
			Assert.IsTrue(obj == DBNull.Value);
		}

		[Test]
		public void NullObjectEqualsNull() {
			Field obj = null;
			Assert.AreEqual(null, obj);

			Field result = null;
			Assert.DoesNotThrow(() => result = obj == null);
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<BooleanType>(result.Type);
			Assert.IsTrue(result.IsNull);
		}
	}
}