using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class RollbackTests : ContextBasedTest {
		private IQuery testQuery;

		protected override bool OnSetUp(string testName, IQuery query) {
			if (testName != "RollbackTableCreate") {
				var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
				tableInfo.AddColumn("a", PrimitiveTypes.Integer());
				tableInfo.AddColumn("b", PrimitiveTypes.String());
				query.Access().CreateObject(tableInfo);
			}

			return true;
		}

		protected override void OnAfterSetup(string testName) {
			testQuery = CreateQuery(CreateAdminSession(Database));

			base.OnAfterSetup(testName);
		}

		protected override void OnBeforeTearDown(string testName) {
			if (testQuery != null)
				testQuery.Dispose();

			base.OnBeforeTearDown(testName);
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			if (testName != "RollbackTableCreate") {
				query.Access().DropObject(DbObjectType.Table, ObjectName.Parse("APP.test_table"));
			}

			return true;
		}

		[Test]
		public void RollbackTableCreate() {
			testQuery.CreateTable(ObjectName.Parse("APP.test_table"), new[] {
				new SqlTableColumn("a", PrimitiveTypes.Integer()),
				new SqlTableColumn("b", PrimitiveTypes.String()) 
			});

			testQuery.Rollback();

			Assert.IsFalse(AdminQuery.Access().TableExists(ObjectName.Parse("APP.test_table")));
		}

		[Test]
		public void RollbackInsert() {
			testQuery.Insert(ObjectName.Parse("APP.test_table"), new SqlExpression[] {
				SqlExpression.Constant(33),
				SqlExpression.Constant("Hello!")
			});

			testQuery.Rollback();

			var table = AdminQuery.Access().GetTable(ObjectName.Parse("APP.test_table"));
			Assert.IsNotNull(table);
			Assert.AreEqual(0, table.RowCount);
		}
	}
}
