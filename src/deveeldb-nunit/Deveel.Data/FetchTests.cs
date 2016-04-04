using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class FetchTests : ContextBasedTest {
		protected override void OnSetUp(string testName, IQuery systemQuery) {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("a", PrimitiveTypes.Integer());
			tableInfo.AddColumn("b", PrimitiveTypes.String(), false);

			systemQuery.Access().CreateTable(tableInfo);

			var table = systemQuery.Access().GetMutableTable(tableName);
			for (int i = 0; i < 10; i++) {
				var row = table.NewRow();
				row.SetValue(0, i);
				row.SetValue(1, String.Format("ID: {0}", i));
				table.AddRow(row);
			}
		}

		protected override void OnAfterSetup(string testName) {
			var query = (SqlQueryExpression)SqlExpression.Parse("SELECT * FROM APP.test_table");

			var cursorInfo = new CursorInfo("c1", query);
			cursorInfo.Flags = CursorFlags.Scroll;

			Query.Access().CreateObject(cursorInfo);
			var c1 = (Cursor)Query.Access().GetObject(DbObjectType.Cursor, new ObjectName("c1"));
			c1.Open();
		}

		protected override void OnTearDown(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			query.Access().DropObject(DbObjectType.Table, tableName);
		}

		[Test]
		public void FetchNext() {
			var row = Query.FetchNext("c1");

			Assert.IsNotNull(row);
			Assert.AreEqual(2, row.ColumnCount);

			var val1 = row.GetValue(0);
			var val2 = row.GetValue(1);

			Assert.IsNotNull(val1);
			Assert.IsNotNull(val2);

			Assert.IsInstanceOf<NumericType>(val1.Type);
			Assert.IsInstanceOf<StringType>(val2.Type);

			var id = ((SqlNumber) val1.Value).ToInt32();
			Assert.AreEqual(0, id);
		}

		[Test]
		public void FetchAbsolute() {
			var row = Query.FetchAbsolute("c1", SqlExpression.Constant(5));

			Assert.IsNotNull(row);
			Assert.AreEqual(2, row.ColumnCount);

			var val1 = row.GetValue(0);
			var val2 = row.GetValue(1);

			Assert.IsNotNull(val1);
			Assert.IsNotNull(val2);

			Assert.IsInstanceOf<NumericType>(val1.Type);
			Assert.IsInstanceOf<StringType>(val2.Type);

			var id = ((SqlNumber)val1.Value).ToInt32();
			Assert.AreEqual(5, id);
		}
	}
}
