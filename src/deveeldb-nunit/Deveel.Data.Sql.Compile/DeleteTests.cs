using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
    [TestFixture]
    public sealed class DeleteTests : SqlCompileTestBase {
        [Test]
        public void FromTable() {
            const string sql = "DELETE FROM table WHERE a = 1";

            var result = Compile(sql);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.HasErrors);

            Assert.AreEqual(1, result.Statements.Count);

            var statement = result.Statements.ElementAt(0);

            Assert.IsNotNull(statement);
            Assert.IsInstanceOf<DeleteStatement>(statement);

            var delete = (DeleteStatement) statement;

            Assert.IsNotNull(delete.TableName);
            Assert.AreEqual("table", delete.TableName.Name);
            Assert.IsNotNull(delete.WhereExpression);
            Assert.AreEqual(-1, delete.Limit);
        }

        [Test]
        public void CurrentFromCursor() {
            const string sql = "DELETE FROM table WHERE CURRENT OF cursor";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DeleteCurrentStatement>(statement);

	        var deleteCurrent = (DeleteCurrentStatement) statement;

			Assert.IsNotNull(deleteCurrent.TableName);
			Assert.AreEqual("table", deleteCurrent.TableName.Name);
			Assert.AreEqual("cursor", deleteCurrent.CursorName);
        }
	}
}