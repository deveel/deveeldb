using System;

using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public class CursorTests : ContextBasedTest {
		protected override IQueryContext CreateQueryContext(IDatabase database) {
			// We first create the table in another context...
			using (var context = database.CreateQueryContext(AdminUserName, AdminPassword)) {
				var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
				tableInfo.AddColumn("a", PrimitiveTypes.Integer());
				tableInfo.AddColumn("b", PrimitiveTypes.String(), false);

				context.CreateTable(tableInfo, false, false);
				context.Commit();
			}

			return base.CreateQueryContext(database);
		}

		[Test]
		public void DeclareInsensitiveCursor() {
			var query = new SqlQueryExpression(new []{new SelectColumn(SqlExpression.Constant("*")) });
			query.FromClause = new FromClause();
			query.FromClause.AddTable("test_table");

			Assert.DoesNotThrow(() => QueryContext.DeclareInsensitiveCursor("c1", query));

			var exists = QueryContext.ObjectExists(DbObjectType.Cursor, new ObjectName("c1"));
			Assert.IsTrue(exists);
		}
	}
}
