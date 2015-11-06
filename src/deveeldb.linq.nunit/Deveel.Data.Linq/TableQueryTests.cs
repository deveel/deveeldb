using System;

using Deveel.Data.Mapping;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Linq {
	[TestFixture]
	public class TableQueryTests : ContextBasedTest {
		private QueryContext Context { get; set; }

		protected override IDatabase CreateDatabase(IDatabaseContext context) {
			var database = base.CreateDatabase(context);

			using (var queryContext = database.CreateQueryContext(AdminUserName, AdminPassword)) {
				CreateTestTable(queryContext);
				AddTestData(queryContext);

				queryContext.Commit();
			}

			return database;
		}

		private void CreateTestTable(IQueryContext context) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.people"));
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
				new SqlExpression[] { SqlExpression.Constant(tableInfo.TableName.FullName) });
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime());
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());

			context.CreateTable(tableInfo);
			context.AddPrimaryKey(tableInfo.TableName, "id", "PK_PEOPLE_TABLE");
		}

		private void AddTestData(IQueryContext context) {
			var table = context.GetMutableTable(ObjectName.Parse("APP.people"));
			var row = table.NewRow();

			// row.SetValue("id", DataObject.Integer(0));
			row.SetDefault(0, context);
			row.SetValue("first_name", DataObject.String("John"));
			row.SetValue("last_name", DataObject.String("Doe"));
			row.SetValue("birth_date", DataObject.Date(new SqlDateTime(1977, 01, 01)));
			row.SetValue("active", DataObject.Boolean(false));
			table.AddRow(row);

			row = table.NewRow();

			// row.SetValue("id", DataObject.Integer(1));
			row.SetDefault(0, context);
			row.SetValue("first_name", DataObject.String("Jane"));
			row.SetValue("last_name", DataObject.String("Doe"));
			row.SetValue("birth_date", DataObject.Date(new SqlDateTime(1978, 11, 01)));
			row.SetValue("active", DataObject.Boolean(true));
			table.AddRow(row);

			row = table.NewRow();

			// row.SetValue("id", DataObject.Integer(2));
			row.SetDefault(0, context);
			row.SetValue("first_name", DataObject.String("Roger"));
			row.SetValue("last_name", DataObject.String("Rabbit"));
			row.SetValue("birth_date", DataObject.Date(new SqlDateTime(1985, 05, 05)));
			row.SetValue("active", DataObject.Boolean(true));
			table.AddRow(row);

			context.Commit();
		}

		protected override void OnSetUp(string testName) {
			Context = new TestQueryContext(QueryContext);
		}

		[Test]
		public void QueryById() {
			Person entity = null;
			Assert.DoesNotThrow(() => entity = Context.Table<Person>().FindById(1));
			Assert.IsNotNull(entity);
			Assert.AreEqual(1, entity.Id);
			Assert.AreEqual("John", entity.FirstName);
		}

		class TestQueryContext : QueryContext {
			public TestQueryContext(IQueryContext context) 
				: base(context) {
			}

			protected override void OnBuildMap(MappingContext mappingContext) {
				mappingContext.Map<Person>()
					.ToTable("people");
				mappingContext.Map<Person>()
					.Member(person => person.Id)
					.HasName("id")
					.IsPrimaryKey();
				mappingContext.Map<Person>()
					.Member(person => person.FirstName)
					.HasName("first_name")
					.IsNotNull();
			}
		}

		#region Person

		class Person {
			public int Id { get; set; }

			public string FirstName { get; set; }
		}

		#endregion
	}
}
