using System;

using Deveel.Data.Mapping;

using NUnit.Framework;

namespace Deveel.Data.Linq {
	[TestFixture]
	public sealed class DbQueryContextTests : ContextBasedTest {
		protected override bool OnTearDown(string testName, IQuery query) {
			if (testName.Equals("CreateDatabaseFromContext")) {
				DropTables(query);
				return true;
			}

			return base.OnTearDown(testName, query);
		}

		private void DropTables(IQuery query) {
			
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
					.HasSize(50);
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
