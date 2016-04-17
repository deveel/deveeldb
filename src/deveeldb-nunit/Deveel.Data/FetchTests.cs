using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class FetchTests : ContextBasedTest {
		protected override void OnAfterSetup(string testName) {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("a", PrimitiveTypes.Integer());
			tableInfo.AddColumn("b", PrimitiveTypes.String(), false);

			Query.Access().CreateTable(tableInfo);

			var table = Query.Access().GetMutableTable(tableName);
			for (int i = 0; i < 10; i++) {
				var row = table.NewRow();
				row.SetValue(0, i);
				row.SetValue(1, String.Format("ID: {0}", i));
				table.AddRow(row);
			}

			var query = (SqlQueryExpression)SqlExpression.Parse("SELECT * FROM APP.test_table");

			var cursorInfo = new CursorInfo("c1", query);
			cursorInfo.Flags = CursorFlags.Scroll;

			Query.Access().CreateObject(cursorInfo);
			var c1 = (Cursor)Query.Access().GetObject(DbObjectType.Cursor, new ObjectName("c1"));
			c1.Open();

			if (testName.StartsWith("FetchInto")) {
				var query2 = (SqlQueryExpression)SqlExpression.Parse("SELECT a FROM APP.test_table");

				var cursorInfo2 = new CursorInfo("c2", query2);
				cursorInfo2.Flags = CursorFlags.Scroll;

				Query.Access().CreateObject(cursorInfo2);
				var c2 = (Cursor)Query.Access().GetObject(DbObjectType.Cursor, new ObjectName("c2"));
				c2.Open();

				Query.Access().CreateObject(new VariableInfo("var1", PrimitiveTypes.BigInt(), false));
				Query.Access().CreateObject(new VariableInfo("var2", PrimitiveTypes.String(), false));
			}
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

		[Test]
		public void FetchIntoOneVar() {
			Query.FetchNextInto("c2", SqlExpression.VariableReference("var1"));

			var var1 = (Variable) Query.Access().GetObject(DbObjectType.Variable, new ObjectName("var1"));

			Assert.IsNotNull(var1);
			Assert.IsNotNull(var1.Value);
			Assert.IsFalse(Field.IsNullField(var1.Value));

			var value = ((SqlNumber) var1.Value.Value).ToInt32();
			Assert.AreEqual(0, value);
		}

		[Test]
		public void FetchIntoTwoVars() {
			var tuple = SqlExpression.Tuple(SqlExpression.VariableReference("var1"), SqlExpression.VariableReference("var2"));

			Query.FetchNextInto("c1", tuple);

			var var1 = (Variable)Query.Access().GetObject(DbObjectType.Variable, new ObjectName("var1"));

			Assert.IsNotNull(var1);
			Assert.IsNotNull(var1.Value);
			Assert.IsFalse(Field.IsNullField(var1.Value));

			var value = ((SqlNumber)var1.Value.Value).ToInt32();
			Assert.AreEqual(0, value);
		}
	}
}
