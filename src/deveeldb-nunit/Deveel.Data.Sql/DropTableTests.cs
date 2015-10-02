using System;

using Deveel.Data;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public class DropTableTests : ContextBasedTest {
		protected override IQueryContext CreateQueryContext(IDatabase database) {
			var context = base.CreateQueryContext(database);
			CreateTestTables(context);
			return context;
		}

		private void CreateTestTables(IQueryContext context) {
			var tn1 = ObjectName.Parse("APP.test_table1");
			var tableInfo1 = new TableInfo(tn1);
			tableInfo1.AddColumn(new ColumnInfo("id", PrimitiveTypes.Integer()));
			tableInfo1.AddColumn(new ColumnInfo("name", PrimitiveTypes.String()));
			tableInfo1.AddColumn(new ColumnInfo("date", PrimitiveTypes.DateTime()));
			context.CreateTable(tableInfo1);
			context.AddPrimaryKey(tn1, "id");
				
			var tn2 = ObjectName.Parse("APP.test_table2");
			var tableInfo2 = new TableInfo(tn2);
			tableInfo2.AddColumn(new ColumnInfo("id", PrimitiveTypes.Integer()));
			tableInfo2.AddColumn(new ColumnInfo("other_id", PrimitiveTypes.Integer()));
			tableInfo2.AddColumn(new ColumnInfo("count", PrimitiveTypes.Integer()));
			context.CreateTable(tableInfo2);
			context.AddPrimaryKey(tn2, "id");
			context.AddForeignKey(tn2, new []{"other_id"}, tn1, new []{"id"}, ForeignKeyAction.Cascade, ForeignKeyAction.Cascade, null);
		}

		[Test]
		public void DropNonReferencedTable() {
			var tableName = ObjectName.Parse("APP.test_table2");
			Assert.DoesNotThrow(() => QueryContext.DropTable(tableName));

			bool exists = false;
			Assert.DoesNotThrow(() => exists = QueryContext.TableExists(tableName));
			Assert.IsFalse(exists);
		}

		[Test]
		public void DropReferencedTable() {
			var tableName = ObjectName.Parse("APP.test_table1");
			Assert.Throws<ConstraintViolationException>(() => QueryContext.DropTable(tableName));

			bool exists = false;
			Assert.DoesNotThrow(() => exists = QueryContext.TableExists(tableName));
			Assert.IsTrue(exists);
		}

		[Test]
		public void DropAllTables() {
			var tableNames = new[] {
				ObjectName.Parse("APP.test_table1"),
				ObjectName.Parse("APP.test_table2"),
			};

			Assert.DoesNotThrow(() => QueryContext.DropTables(tableNames));
		}
	}
}
