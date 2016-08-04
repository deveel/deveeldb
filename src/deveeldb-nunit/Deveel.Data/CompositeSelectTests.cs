using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public class CompositeSelectTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			CreateTable(query);
			InsertData(query);
			return true;
		}

		private void CreateTable(IQuery query) {
			var tableName = ObjectName.Parse("APP.persons");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn(new ColumnInfo("id", PrimitiveTypes.BigInt()) {
				IsNotNull = true,
				DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY", new SqlExpression[] {
					SqlExpression.Constant(tableName.FullName)
				})
			});
			tableInfo.AddColumn("name", PrimitiveTypes.String());
			tableInfo.AddColumn("age", PrimitiveTypes.Integer());
			query.Access().CreateTable(tableInfo);
		}

		private void InsertData(IQuery query) {
			var tableName = ObjectName.Parse("APP.persons");
			var table = query.Access().GetMutableTable(tableName);
			var row = table.NewRow();
			row["name"] = Field.String("Antonello Provenzano");
			row["age"] = Field.Integer(36);
			row.SetDefault(query);
			table.AddRow(row);

			row = table.NewRow();
			row["name"] = Field.String("Sebastiano Provenzano");
			row["age"] = Field.Integer(35);
			row.SetDefault(query);
			table.AddRow(row);

			row = table.NewRow();
			row["name"] = Field.String("Mart Rosmaa");
			row["age"] = Field.Integer(33);
			row.SetDefault(query);
			table.AddRow(row);

			row = table.NewRow();
			row["name"] = Field.String("Karl Inge Stensson");
			row["age"] = Field.Integer(54);
			row.SetDefault(query);
			table.AddRow(row);
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.persons");
			query.Access().DropObject(DbObjectType.Table, tableName);
			return true;
		}

		private ITable Execute(string s) {
			var query = (SqlQueryExpression)SqlExpression.Parse(s);
			var result = AdminQuery.Select(query);
			result.GetEnumerator().MoveNext();
			return result.Source;
		}


		[Test]
		public void UnionAll() {
			const string sql = @"SELECT * FROM persons WHERE age < 50 UNION ALL SELECT * FROM persons WHERE age > 50";

			var result = Execute(sql);

			Assert.IsNotNull(result);
			Assert.AreEqual(4, result.RowCount);
		}

		[Test]
		public void Inserset() {
			const string sql = @"SELECT * FROM persons EXCEPT SELECT * FROM persons WHERE age > 50";

			Assert.Throws<NotSupportedException>(() => Execute(sql));
		}
	}
}
