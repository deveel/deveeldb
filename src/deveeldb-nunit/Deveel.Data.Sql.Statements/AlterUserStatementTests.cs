using System;

using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public sealed class AlterUserStatementTests : ContextBasedTest {
		protected override void OnSetUp(string testName) {
			Query.CreateUser("test_user", "12345");
		}

		[Test]
		public void SetPassword() {
			var password = SqlExpression.Constant("abc12345");
			var action = new SetPasswordAction(password);
			var statement = new AlterUserStatement("test_user", action);

			var result = Query.ExecuteStatement(statement);

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
			Assert.AreEqual(1, result.TableInfo.ColumnCount);
			Assert.AreEqual(0, ((SqlNumber)result.GetValue(0, 0).Value).ToInt32());

			//TODO: Assess the password was changed
		}

		[Test]
		public void SetStatusLocked() {
			var action = new SetAccountStatusAction(UserStatus.Locked);
			var statement = new AlterUserStatement("test_user", action);

			var result = Query.ExecuteStatement(statement);

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
			Assert.AreEqual(1, result.TableInfo.ColumnCount);
			Assert.AreEqual(0, ((SqlNumber)result.GetValue(0, 0).Value).ToInt32());

			// TODO: assess the status has changed
		}

		[Test]
		public void SetUserGroups() {
			var groups = new SqlExpression[] {SqlExpression.Constant("secure")};
			var action = new SetUserGroupsAction(groups);
			var statement = new AlterUserStatement("test_user", action);

			var result = Query.ExecuteStatement(statement);

			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
			Assert.AreEqual(1, result.TableInfo.ColumnCount);
			Assert.AreEqual(0, ((SqlNumber)result.GetValue(0, 0).Value).ToInt32());

			// TODO: assess the user is in the group
		}
	}
}
