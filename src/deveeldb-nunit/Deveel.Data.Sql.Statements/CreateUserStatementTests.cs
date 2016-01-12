using System;

using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public sealed class CreateUserStatementTests : ContextBasedTest {
		[Test]
		public void WithSimplePassword() {
			const string userName = "tester";
			var password = SqlExpression.Constant(DataObject.VarChar("12345"));
			var statement = new CreateUserStatement(userName, password);

			statement.Execute(Query);

			var exists = Query.UserExists(userName);
			Assert.IsTrue(exists);
		}
	}
}
