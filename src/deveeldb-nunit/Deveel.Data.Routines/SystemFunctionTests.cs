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

using Deveel.Data;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Routines {
	[TestFixture]
	public class SystemFunctionTests : ContextBasedTest {
		private Field InvokeFunction(string name) {
			return Query.InvokeSystemFunction(name);
		}

		private Field InvokeFunction(string name, Field arg) {
			return Query.InvokeSystemFunction(name, SqlExpression.Constant(arg));
		}

		[Test]
		public void ResolveSystemFunctionWithNoSchema() {
			IFunction function = null;
			Assert.DoesNotThrow(() => function = Query.ResolveFunction(new ObjectName("user")));
			Assert.IsNotNull(function);
			Assert.AreEqual(SystemSchema.Name, function.FullName.ParentName);
			Assert.AreEqual("user", function.FullName.Name);
		}

		[Test]
		public void ResolveSystemFunctionFullyQualified() {
			IFunction function = null;
			Assert.DoesNotThrow(() => function = Query.ResolveFunction(ObjectName.Parse("SYSTEM.user")));
			Assert.IsNotNull(function);
			Assert.AreEqual(SystemSchema.Name, function.FullName.ParentName);
			Assert.AreEqual("user", function.FullName.Name);
		}

		[Test]
		public void InvokeUserFunction() {
			Field result = null;
			Assert.DoesNotThrow(() => result = InvokeFunction("user"));
			Assert.IsNotNull(result);
			Assert.AreEqual(AdminUserName, result.Value.ToString());
		}

		[Test]
		public void InvokeIntegerToString() {
			var value = Field.Integer(455366);
			Field result = null;
			Assert.DoesNotThrow(() => result = InvokeFunction("TOSTRING", value));
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<StringType>(result.Type);

			var stringResult = result.Value.ToString();
			Assert.AreEqual("455366", stringResult);
		}

		[Test]
		public void InvokeDateToString() {
			var value = Field.Date(new SqlDateTime(2015, 02, 10));
			Field result = null;
			Assert.DoesNotThrow(() => result = InvokeFunction("TOSTRING", value));
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<StringType>(result.Type);

			var stringResult = result.Value.ToString();
			Assert.AreEqual("2015-02-10", stringResult);
		}

		[Test]
		public void InvokeTimeStampToString_NoFormat() {
			var value = Field.TimeStamp(new SqlDateTime(2015, 02, 10, 17, 15, 01,00));
			Field result = null;
			Assert.DoesNotThrow(() => result = InvokeFunction("TOSTRING", value));
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<StringType>(result.Type);

			var stringResult = result.Value.ToString();
			Assert.AreEqual("2015-02-10T17:15:01.000 +00:00", stringResult);
		}
	}
}
