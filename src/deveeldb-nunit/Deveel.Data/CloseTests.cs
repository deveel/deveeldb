using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Deveel.Data {
	[TestFixture]
	public sealed class CloseTests : ContextBasedTest {
		protected override void OnAfterSetup(string testName) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			tableInfo.AddColumn("a", PrimitiveTypes.Integer());
			tableInfo.AddColumn("b", PrimitiveTypes.String(), false);
			Query.Access().CreateTable(tableInfo);
			DeclareCursors(Query);
			OpenCursors(Query);
		}

		private static void DeclareCursors(IQuery query) {
			var queryExp1 = (SqlQueryExpression)SqlExpression.Parse("SELECT * FROM APP.test_table");
			query.Context.DeclareCursor(query, "c1", queryExp1);


			var queryExp2 = (SqlQueryExpression)SqlExpression.Parse("SELECT * FROM APP.test_table WHERE a > :a");
			var cursorInfo = new CursorInfo("c2", queryExp2);
			cursorInfo.Parameters.Add(new CursorParameter("a", PrimitiveTypes.Integer()));
			query.Context.DeclareCursor(cursorInfo, query);
		}

		private static void OpenCursors(IQuery query) {
			var cursor = (Cursor) query.Access().FindObject(new ObjectName("c1"));
			cursor.Open();
		}

		[Test]
		public void TypicalClode() {
			Query.Close("c1");

			var cursor = (Cursor)Query.Access().FindObject(new ObjectName("c1"));
			Assert.AreEqual(CursorStatus.Closed, cursor.Status);
		}

		[Test]
		public void CloseNotOpened() {
			Assert.DoesNotThrow(() => Query.Close("c2"));
		}
	}
}
