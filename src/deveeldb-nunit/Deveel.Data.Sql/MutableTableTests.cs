using System;

using Deveel.Data.Configuration;
using Deveel.Data.DbSystem;
using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public class MutableTableTests {
		private IUserSession session;
		private SessionQueryContext context;

		[SetUp]
		public void SetUp() {
			var systemContext = new SystemContext(DbConfig.Default);
			var dbContext = new DatabaseContext(systemContext, "testdb");
			var database = new Database(dbContext);
			database.Create("SA", "12345");
			database.Open();

			session = database.CreateSession(User.System);
			context = new SessionQueryContext(session);

			var test = TestContext.CurrentContext.Test;

			var table = CreateTable();

			if (test.Name != "InsertInto") {
				InsertIntoTable(table);
			}
		}

		private void InsertIntoTable(IMutableTable table) {
			var row = table.NewRow();
			row.SetValue(0,  "Antonello Provenzano");
			row.SetValue(1, 33);
			row.SetValue(2, 0);
			table.AddRow(row);

			row = table.NewRow();
			row.SetValue(0, "Maart Roosmaa");
			row.SetValue(1, 28);
			row.SetValue(2, 5);
			table.AddRow(row);

			row = table.NewRow();
			row.SetValue(0, "Rezaul Horaque");
			row.SetValue(1, 27);
			row.SetValue(2, 2);
			table.AddRow(row);
		}

		private IMutableTable CreateTable() {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("name", PrimitiveTypes.String(), true);
			tableInfo.AddColumn("age", PrimitiveTypes.Integer());
			tableInfo.AddColumn("order", PrimitiveTypes.Integer());

			session.CreateTable(tableInfo);
			return session.GetMutableTable(tableName);
		}

		[TearDown]
		public void TearDown() {

		}

		[Test]
		public void SameSessionInsertInto() {
			Assert.Inconclusive();
		}

		[Test]
		public void SameSessionDeleteFrom() {
			var queryExpression = SqlExpression.GreaterThan(SqlExpression.Reference(new ObjectName("order")), SqlExpression.Constant(2));

			int deleteCount = -1;
			Assert.DoesNotThrow(() => deleteCount = context.DeleteFrom(ObjectName.Parse("APP.test_table"), queryExpression));
			Assert.AreEqual(1, deleteCount);
		}
	}
}
