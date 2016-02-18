using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public class DropUserTests : SqlCompileTestBase {
		[Test]
		public void DropSingleUser() {
			const string sql = "DROP USER tester";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.CodeObjects.Count);

			var statement = result.CodeObjects.First();

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DropUserStatement>(statement);

			var dropUser = (DropUserStatement) statement;

			Assert.AreEqual("tester", dropUser.UserName);
		}

		[Test]
		public void DropMultipleUsers() {
			const string sql = "DROP USER tester1, tester2";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(2, result.CodeObjects.Count);
		}
	}
}