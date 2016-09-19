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
		private SessionQueryContext Context { get; set; }

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
			AdminSession.Context.RegisterInstance<IMappingContext>(new TestMappingContext());
			Context = new SessionQueryContext(AdminSession);
		}

		protected override void OnBeforeTearDown(string testName) {
			if (Context != null)
				Context.Dispose();
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			query.Access().DropObject(DbObjectType.Table, tableName);
			return true;
		}

		[Test]
		public void QueryFirstOrDefault() {
			var result = Context.Table<TestClass>()
				.FirstOrDefault();

			Assert.IsNotNull(result);
			Assert.AreEqual("Antonello Provenzano", result.Name);
		}

		[Test]
		public void QueryAgeGreaterThan() {
			var result = Context.Table<TestClass>()
				.Where(x => x.Age > 32)
				.ToList();

			Assert.IsNotEmpty(result);
			Assert.AreEqual(1, result.Count);

			var first = result.ElementAt(0);

			Assert.IsNotNull(first);
			Assert.AreEqual("Antonello Provenzano", first.Name);
		}

		[Test]
		public void QueryLastOrDefault() {
			TestClass result = null;
			Assert.Throws<NotSupportedException>(() => result = Context.Table<TestClass>().LastOrDefault());

			result = Context.Table<TestClass>().AsEnumerable().LastOrDefault();

			Assert.IsNotNull(result);
			Assert.AreEqual("Mart Roosmaa", result.Name);
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
