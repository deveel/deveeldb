using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class OpenCursorTests : ContextBasedTest {
		protected override void OnSetUp(string testName, IQuery query) {
			CreateTable(query);
		}

		private static void CreateTable(IQuery query) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			tableInfo.AddColumn("a", PrimitiveTypes.Integer());
			tableInfo.AddColumn("b", PrimitiveTypes.String(), false);
			query.Access().CreateTable(tableInfo);
		}

		private static void DeclareCursors(IQuery query) {
			var queryExp1 = (SqlQueryExpression) SqlExpression.Parse("SELECT * FROM APP.test_table");
			query.Context.DeclareCursor(query, "c1", queryExp1);


			var queryExp2 = (SqlQueryExpression)SqlExpression.Parse("SELECT * FROM APP.test_table WHERE a > :a");
			var cursorInfo = new CursorInfo("c2", queryExp2);
			cursorInfo.Parameters.Add(new CursorParameter("a", PrimitiveTypes.Integer()));
			query.Context.DeclareCursor(cursorInfo,query);
		}

		protected override void OnAfterSetup(string testName) {
			DeclareCursors(Query);
		}

		[Test]
		public void Simple() {
			Query.Open("c1");

			var cursor = Query.Context.FindCursor("c1");
			Assert.IsNotNull(cursor);
			Assert.AreEqual("c1", cursor.CursorInfo.CursorName);
			Assert.AreEqual(CursorStatus.Open, cursor.Status);
		}
	}
}
