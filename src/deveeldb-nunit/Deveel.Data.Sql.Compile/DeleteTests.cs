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
    public sealed class DeleteTests : SqlCompileTestBase {
        [Test]
        public void FromTable() {
            const string sql = "DELETE FROM table1 WHERE a = 1";

            var result = Compile(sql);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.HasErrors);

            Assert.AreEqual(1, result.Statements.Count);

            var statement = result.Statements.ElementAt(0);

            Assert.IsNotNull(statement);
            Assert.IsInstanceOf<DeleteStatement>(statement);

            var delete = (DeleteStatement) statement;

            Assert.IsNotNull(delete.TableName);
            Assert.AreEqual("table1", delete.TableName.Name);
            Assert.IsNotNull(delete.WhereExpression);
            Assert.AreEqual(-1, delete.Limit);
        }

		[Test]
	    public void WithLimit() {
			const string sql = "DELETE FROM table1 WHERE a = 1 LIMIT 23";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DeleteStatement>(statement);

			var delete = (DeleteStatement)statement;

			Assert.IsNotNull(delete.TableName);
			Assert.AreEqual("table1", delete.TableName.Name);
			Assert.IsNotNull(delete.WhereExpression);
			Assert.AreEqual(23, delete.Limit);
		}

        [Test]
        public void CurrentFromCursor() {
            const string sql = "DELETE FROM table1 WHERE CURRENT OF cursor";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DeleteCurrentStatement>(statement);

	        var deleteCurrent = (DeleteCurrentStatement) statement;

			Assert.IsNotNull(deleteCurrent.TableName);
			Assert.AreEqual("table1", deleteCurrent.TableName.Name);
			Assert.AreEqual("cursor", deleteCurrent.CursorName);
        }
	}
}