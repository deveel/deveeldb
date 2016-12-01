using System;

using Deveel.Data.Design;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Linq {
	[TestFixture]
	public sealed class InsertTests : ContextBasedTest {
		private SessionQueryContext Context { get; set; }

		protected override bool OnSetUp(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			var tableInfo = new TableInfo(tableName);
			var id = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			id.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
				new SqlExpression[] { SqlExpression.Constant(tableName.FullName) });

			tableInfo.AddColumn("name", PrimitiveTypes.VarChar());
			tableInfo.AddColumn("age", PrimitiveTypes.Integer());

			query.Access().CreateObject(tableInfo);
			return true;
		}

		protected override void OnAfterSetup(string testName) {
			Context = new TestQueryContext(AdminSession);
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
		public void InsertNew() {
			Context.Table<TestClass>()
				.Insert(new TestClass {
					Name = "Antonello Provenzano",
					Age = 35
				});

			var table = AdminQuery.Access().GetTable(ObjectName.Parse("APP.test_table"));

			Assert.AreEqual(1, table.RowCount);
		}

		#region TestClass

		[TableName("test_table")]
		class TestClass {
			[Column(Name = "id")]
			[DefaultExpression("UNIQUEKEY(test_table)")]
			[PrimaryKey]
			public int Id { get; set; }

			[Column(Name = "name")]
			public string Name { get; set; }

			[Column(Name = "age")]
			public int Age { get; set; }
		}

		#endregion

		#region TestModelBuildContext

		class TestQueryContext : SessionQueryContext {
			public TestQueryContext(ISession session)
				: base(session) {
			}

			protected override void OnBuildModel(DbModelBuilder modelBuilder) {
				modelBuilder.Type<TestClass>();
			}
		}

		#endregion
	}
}
