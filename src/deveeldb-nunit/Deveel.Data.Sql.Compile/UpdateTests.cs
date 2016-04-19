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

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public class UpdateTests : SqlCompileTestBase {
		[Test]
		public void SimpleUpdate() {
			const string sql = "UPDATE table1 SET col1 = 'testUpdate', col2 = 22 WHERE id = 1";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<UpdateStatement>(statement);

			var update = (UpdateStatement) statement;

			Assert.IsNotNull(update.TableName);
			Assert.AreEqual("table1", update.TableName.FullName);

			Assert.IsNotEmpty(update.Assignments);
			Assert.IsNotNull(update.WherExpression);
			Assert.AreEqual(-1, update.Limit);
		}

		[Test]
		public void SimpleUpdateWithLimit() {
			const string sql = "UPDATE table1 SET col1 = 'testUpdate', col2 = 22 WHERE id = 1 LIMIT 20";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.FirstOrDefault();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<UpdateStatement>(statement);

			var update = (UpdateStatement) statement;

			Assert.IsNotNull(update.TableName);
			Assert.AreEqual("table1", update.TableName.FullName);

			Assert.IsNotEmpty(update.Assignments);
			Assert.IsNotNull(update.WherExpression);
			Assert.AreEqual(20, update.Limit);
		}
	}
}