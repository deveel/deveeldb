using System;
using System.Linq;

using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Tables {
	[TestFixture]
	public sealed class ColumnTests : ContextBasedTest {
		private ObjectName tableName = ObjectName.Parse("APP.test_table");

		protected override bool OnSetUp(string testName, IQuery query) {
			CreateTable(query);
			AddTestData(query);
			return true;
		}

		private void AddTestData(IQuery query) {
			var table = query.Access().GetMutableTable(tableName);
			for (int i = 1; i <= 200; i++) {
				var row = table.NewRow();
				row["id"] = Field.Integer(i);
				row["name"] = Field.String(String.Format("n_{0}", i));
				table.AddRow(row);
			}
		}

		private void CreateTable(IQuery query) {
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("name", PrimitiveTypes.String());

			query.Access().CreateObject(tableInfo);
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			query.Access().DropObject(DbObjectType.Table, tableName);
			return true;
		}

		[TestCase(0, "id")]
		[TestCase(1, "name")]
		public void ColumnName(int columnOffset, string columnName) {
			var table = AdminQuery.Access().GetTable(tableName);

			Assert.IsNotNull(table);

			var column = table.GetColumn(columnOffset);
			Assert.IsNotNull(column);
			Assert.IsNotNull(column.ColumnInfo);
			Assert.IsNotNull(column.Table);
			Assert.IsNotNull(column.Index);
			Assert.AreEqual(columnName, column.Name);
			Assert.AreEqual(columnOffset, column.Offset);
		}

		[TestCase("id", 34, 35)]
		[TestCase("name", 58, "n_59")]
		public void ColumnValueAt(string columnName, int rowOffset, object value) {
			var table = AdminQuery.Access().GetTable(tableName);

			Assert.IsNotNull(table);

			var column = table.GetColumn(columnName);
			Assert.IsNotNull(column);
			Assert.IsNotNull(column.ColumnInfo);
			Assert.IsNotNull(column.Table);
			Assert.IsNotNull(column.Index);
			Assert.AreEqual(columnName, column.Name);

			var fieldValue = new Field(column.Type, column.Type.CreateFrom(value));
			var cellValue = column.GetValue(rowOffset);

			Assert.IsFalse(Field.IsNullField(cellValue));
			Assert.IsTrue(fieldValue.Equals(cellValue));
		}

		[TestCase("name", 23, "n_24")]
		public void EnumerateColumnValues(string columnName, int rowOffset, object expected) {
			var table = AdminQuery.Access().GetTable(tableName);

			Assert.IsNotNull(table);

			var column = table.GetColumn(columnName);
			Assert.IsNotNull(column);
			Assert.IsNotNull(column.ColumnInfo);
			Assert.IsNotNull(column.Table);
			Assert.IsNotNull(column.Index);
			Assert.AreEqual(columnName, column.Name);

			var cellValue = column.ElementAt(rowOffset);
			var fieldValue = new Field(column.Type, column.Type.CreateFrom(expected));

			Assert.IsFalse(Field.IsNullField(cellValue));
			Assert.IsTrue(fieldValue.Equals(cellValue));

		}
	}
}
