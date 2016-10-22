using System;
using System.Linq;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;
using Deveel.Data.Transactions;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public class LockTableTests : ContextBasedTest {
		protected override bool CreateTestUser {
			get { return true; }
		}

		protected override bool OnSetUp(string testName, IQuery query) {
			query.Access().CreateTable(table => table
				.Named("APP.test1")
				.WithColumn("id", PrimitiveTypes.Integer())
				.WithColumn("name", PrimitiveTypes.String()));

			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test1");
			query.Access().DropObject(DbObjectType.Table, tableName);
			return true;
		}

		protected override void OnAfterSetup(string testName) {
			UserQuery.Set(TransactionSettingKeys.LockTimeout, 200);
		}

		[Test]
		public void LockExclusiveAndAccess() {
			var tableName = ObjectName.Parse("APP.test1");
			AdminQuery.LockTable(tableName, LockingMode.Exclusive);

			var expected = Is.InstanceOf<TransactionException>()
				.And.TypeOf<LockTimeoutException>()
				.And.Property("TableName").EqualTo(tableName)
				.And.Property("AccessType").EqualTo(AccessType.Write);

			var query = (SqlQueryExpression) SqlExpression.Parse("SELECT * FROM APP.test1");
			Row row;
			Assert.Throws(expected, () => row = UserQuery.Select(query).FirstOrDefault());
		}
	}
}
