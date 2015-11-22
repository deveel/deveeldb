using System;

using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public class CursorTests : ContextBasedTest {
		protected override IUserSession CreateAdminSession(IDatabase database) {
			using (var session = base.CreateAdminSession(database)) {
				using (var query = session.CreateQuery()) {
					var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
					tableInfo.AddColumn("a", PrimitiveTypes.Integer());
					tableInfo.AddColumn("b", PrimitiveTypes.String(), false);

					query.CreateTable(tableInfo, false, false);
					query.Commit();
				}
			}

			return base.CreateAdminSession(database);
		}

		[Test]
		public void DeclareInsensitiveCursor() {
			var query = new SqlQueryExpression(new []{new SelectColumn(SqlExpression.Constant("*")) });
			query.FromClause = new FromClause();
			query.FromClause.AddTable("test_table");

			Assert.DoesNotThrow(() => Query.DeclareInsensitiveCursor("c1", query));

			var exists = Query.ObjectExists(DbObjectType.Cursor, new ObjectName("c1"));
			Assert.IsTrue(exists);
		}
	}
}
