using System;
using System.Linq;

using Deveel.Data.Design;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Linq {
	[TestFixture]
	public class LinqTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			var id = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			id.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
				new SqlExpression[] {SqlExpression.Constant(tableName.FullName)});

			tableInfo.AddColumn("name", PrimitiveTypes.VarChar());
			tableInfo.AddColumn("age", PrimitiveTypes.Integer());

			query.Access().CreateObject(tableInfo);

			AddTestData(query, tableName);
			return true;
		}

		private void AddTestData(IQuery query, ObjectName tableName) {
			var table = query.Access().GetMutableTable(tableName);
			var row = table.NewRow();
			row["name"] = Field.String("Antonello Provenzano");
			row["age"] = Field.Integer(35);
			row.SetDefault(query);
			table.AddRow(row);

			row = table.NewRow();
			row["name"] = Field.String("Mart Roosmaa");
			row["age"] = Field.Integer(30);
			row.SetDefault(query);
			table.AddRow(row);
		}

		protected override void OnAfterSetup(string testName) {
			AdminQuery.Context.RegisterInstance<IMappingContext>(new TestMappingContext());
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			query.Access().DropObject(DbObjectType.Table, tableName);
			return true;
		}

		[Test]
		public void QueryFirstOrDefault() {
			var result = AdminQuery.AsQueryable<TestClass>()
				.FirstOrDefault();

			Assert.IsNotNull(result);
			Assert.AreEqual("Antonello Provenzano", result.Name);
		}

		#region TestClass

		[TableName("test_table")]
		class TestClass {
			[Column(Name = "id")]
			[DefaultExpression("UNIQUEKEY(test_table)")]
			public int Id { get; set; }

			[Column(Name = "name")]
			public string Name { get; set; }

			[Column(Name = "age")]
			public int Age { get; set; }
		}

		#endregion

		#region TestMappingContext

		class TestMappingContext : IMappingContext {
			public void OnBuildMap(MapModelBuilder builder) {
				builder.Type<TestClass>();
			}
		}

		#endregion
	}
}
