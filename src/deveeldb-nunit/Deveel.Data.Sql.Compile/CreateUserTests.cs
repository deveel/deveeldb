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
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class CreateUserTests : SqlCompileTestBase {
		[Test]
		public void IdentifiedByPassword() {
			const string sql = "CREATE USER test IDENTIFIED BY PASSWORD '123456789';";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateUserStatement>(statement);

			var createUser = (CreateUserStatement) statement;

			Assert.AreEqual("test", createUser.UserName);
			Assert.IsNotNull(createUser.Identifier);
			Assert.AreEqual(SqlIdentificationType.Password, createUser.Identifier.Type);
			Assert.IsInstanceOf<SqlConstantExpression>(createUser.Identifier.Argument);
		}

		[Test]
		public void IdentifiedExternally() {
			const string sql = "CREATE USER test IDENTIFIED EXTERNALLY AS ('db_test');";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateUserStatement>(statement);

			var createUser = (CreateUserStatement)statement;

			Assert.AreEqual("test", createUser.UserName);
			Assert.AreEqual(SqlIdentificationType.External, createUser.Identifier.Type);
		}
	}
}
