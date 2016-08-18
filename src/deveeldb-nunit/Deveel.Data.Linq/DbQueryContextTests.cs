using System;
using System.Linq;

using Deveel.Data.Mapping;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Linq {
	[TestFixture]
	public sealed class DbQueryContextTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			if (testName.Equals("SelectFromTables")) {
				CreateTables(query);
				InsertTestData(query);
				return true;
			}

			return base.OnSetUp(testName, query);
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			if (testName.Equals("SelectFromTables")) {
				DropTables(query);
				return true;
			}

			return base.OnTearDown(testName, query);
		}

		private void CreateTables(IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());

			query.Access().CreateTable(tableInfo);
		}

		private void InsertTestData(IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			var table = query.Access().GetMutableTable(tableName);

			var row = table.NewRow();
			row["id"] = Field.Integer(0);
			row["first_name"] = Field.String("Antonello");
			row["last_name"] = Field.String("Provenzano");
			table.AddRow(row);

			row = table.NewRow();
			row["id"] = Field.Integer(1);
			row["first_name"] = Field.String("Sebastiano");
			row["last_name"] = Field.String("Provenzano");
			table.AddRow(row);
		}

		private void DropTables(IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			query.Access().DropObject(DbObjectType.Table, tableName);
		}

		[Test]
		public void CreateNew() {
			DbQueryContext queryContext = null;
			Assert.DoesNotThrow(() => queryContext = new DbQueryContext(AdminQuery));
			Assert.IsNotNull(queryContext);
		}

		[Test]
		public void ObtainTable() {
			DbQueryContext queryContext = null;
			Assert.DoesNotThrow(() => queryContext = new DbQueryContext(AdminQuery));
			Assert.IsNotNull(queryContext);

			DbTable<TestClass> table = null;

			Assert.DoesNotThrow(() => table = queryContext.Table<TestClass>());
			Assert.IsNotNull(table);
		}

		[Test]
		public void BuildModel() {
			DbQueryContext queryContext = null;
			Assert.DoesNotThrow(() => queryContext = new TestDbQueryContext(AdminQuery));
			Assert.IsNotNull(queryContext);

			DbTable<TestClass> table = null;

			Assert.DoesNotThrow(() => table = queryContext.Table<TestClass>());
			Assert.IsNotNull(table);
		}

		[Test]
		public void SelectFromTables() {
			DbQueryContext queryContext = null;
			Assert.DoesNotThrow(() => queryContext = new TestDbQueryContext(AdminQuery));
			Assert.IsNotNull(queryContext);

			DbTable<TestClass> table = null;

			Assert.DoesNotThrow(() => table = queryContext.Table<TestClass>());
			Assert.IsNotNull(table);

			var result = table.Where(x => x.LastName == "Provenzano").ToList();

			Assert.IsNotEmpty(result);
			Assert.AreEqual(2, result.Count);
		}

		#region TestDbQueryContext

		class TestDbQueryContext : DbQueryContext {
			public TestDbQueryContext(IQuery context) 
				: base(context) {
			}

			protected override void OnBuildModel(DbModelBuilder modelBuilder) {
				modelBuilder.Type<TestClass>()
					.HasKey(x => x.Id)
					.OfType(KeyType.Identity);

				modelBuilder.Type<TestClass>()
					.Member(x => x.FirstName)
					.HasColumnName("first_name")
					.HasSize(50);

				modelBuilder.Type<TestClass>()
					.Member(x => x.LastName)
					.HasColumnName("last_name")
					.Nullable(false)
					.IsMaxSize();
			}
		}

		#endregion

		#region TestClass

		[TableName("test_table")]
		class TestClass {
			public int Id { get; set; }

			public string FirstName { get; set; }

			public string LastName { get; set; }
		}

		#endregion
	}
}
