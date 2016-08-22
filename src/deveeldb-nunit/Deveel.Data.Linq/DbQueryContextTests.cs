using System;
using System.Collections.Generic;
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
			if (testName.StartsWith("SelectFromTables")) {
				CreateTables(query);
				InsertTestData(query);
				return true;
			}

			return base.OnSetUp(testName, query);
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			if (testName.StartsWith("SelectFromTables")) {
				DropTables(query);
				return true;
			}

			return base.OnTearDown(testName, query);
		}

		private void CreateTables(IQuery query) {
			var tableName = ObjectName.Parse("APP.persons");
			var tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());

			query.Access().CreateTable(tableInfo);

			tableName = ObjectName.Parse("APP.person_class");
			tableInfo = new TableInfo(tableName);
			tableInfo.AddColumn("person_id", PrimitiveTypes.Integer());
			tableInfo.AddColumn("class_name", PrimitiveTypes.String());

			query.Access().CreateTable(tableInfo);
		}

		private void InsertTestData(IQuery query) {
			var tableName = ObjectName.Parse("APP.persons");
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

			tableName = ObjectName.Parse("APP.person_class");
			table = query.Access().GetMutableTable(tableName);

			row = table.NewRow();
			row["person_id"] = Field.Integer(0);
			row["class_name"] = Field.String("Systems and Networks");
			table.AddRow(row);
		}

		private void DropTables(IQuery query) {
			var tableName = ObjectName.Parse("APP.persons");
			query.Access().DropObject(DbObjectType.Table, tableName);

			tableName = ObjectName.Parse("APP.person_class");
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

			DbTable<Person> table = null;

			Assert.DoesNotThrow(() => table = queryContext.Table<Person>());
			Assert.IsNotNull(table);
		}

		[Test]
		public void BuildModel() {
			DbQueryContext queryContext = null;
			Assert.DoesNotThrow(() => queryContext = new TestDbQueryContext(AdminQuery));
			Assert.IsNotNull(queryContext);

			DbTable<Person> table = null;

			Assert.DoesNotThrow(() => table = queryContext.Table<Person>());
			Assert.IsNotNull(table);
		}

		[Test]
		public void SelectFromTables() {
			DbQueryContext queryContext = null;
			Assert.DoesNotThrow(() => queryContext = new TestDbQueryContext(AdminQuery));
			Assert.IsNotNull(queryContext);

			DbTable<Person> table = null;

			Assert.DoesNotThrow(() => table = queryContext.Table<Person>());
			Assert.IsNotNull(table);

			var result = table.Where(x => x.LastName == "Provenzano").ToList();

			Assert.IsNotEmpty(result);
			Assert.AreEqual(2, result.Count);

			var first = result[0];
			Assert.AreEqual("Antonello", first.FirstName);
			Assert.AreEqual("Provenzano", first.LastName);
		}

		[Test]
		public void SelectFromTables_LinqExpr() {
			DbQueryContext queryContext = null;
			Assert.DoesNotThrow(() => queryContext = new TestDbQueryContext(AdminQuery));
			Assert.IsNotNull(queryContext);

			DbTable<Person> table = null;

			Assert.DoesNotThrow(() => table = queryContext.Table<Person>());
			Assert.IsNotNull(table);

			var query = (from person in table where person.LastName == "Provenzano" select person);

			var result = query.ToList();

			Assert.IsNotEmpty(result);
			Assert.AreEqual(2, result.Count);

			var first = result[0];
			Assert.AreEqual("Antonello", first.FirstName);
			Assert.AreEqual("Provenzano", first.LastName);
		}


		#region TestDbQueryContext

		class TestDbQueryContext : DbQueryContext {
			public TestDbQueryContext(IQuery context) 
				: base(context) {
			}

			protected override void OnBuildModel(DbModelBuilder modelBuilder) {
				modelBuilder.Type<Person>()
					.HasKey(x => x.Id)
					.OfType(KeyType.Identity);

				modelBuilder.Type<Person>()
					.Member(x => x.FirstName)
					.HasColumnName("first_name")
					.HasSize(50);

				modelBuilder.Type<Person>()
					.Member(x => x.LastName)
					.HasColumnName("last_name")
					.Nullable(false)
					.IsMaxSize();
			}
		}

		#endregion

		#region Person

		[TableName("persons")]
		class Person {
			public int Id { get; set; }

			public string FirstName { get; set; }

			public string LastName { get; set; }

			public ICollection<PersonClass> Classes { get; set; }
		}

		#endregion

		#region PersonClass

		[TableName("person_class")]
		class PersonClass {
			public int PersonId { get; set; }

			public Person Person { get; set; }

			public string ClassName { get; set; }

			public bool Active { get; set; }
		}

		#endregion
	}
}
