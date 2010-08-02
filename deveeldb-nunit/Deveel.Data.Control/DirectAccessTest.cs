using System;

using NUnit.Framework;

namespace Deveel.Data.Control {
	[TestFixture]
	public sealed class DirectAccessTest : TestBase {
		private DbDirectAccess directAccess;

		protected override void OnSetUp() {
			directAccess = System.GetDirectAccess(AdminUser, AdminPassword);
		}

		protected override void OnTearDown() {
			directAccess.Dispose();
		}

		[Test(Description = "Creates a new table using the Direct-Access API")]
		public void CreateTable() {
			/*
			TODO:
			try {
				TableName tableName = new TableName("DA_TestTable");
				DataTableDef tableDef = new DataTableDef();
				tableDef.TableName = tableName;
				tableDef.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
				tableDef.AddColumn(DataTableColumnDef.CreateNumericColumn("count"));
				tableDef.AddColumn(DataTableColumnDef.CreateBooleanColumn("is_set"));
				directAccess.CreateTable(tableDef, true);

				directAccess.AddConstraint(tableName, DataTableConstraintDef.PrimaryKey("DA_TestTable_PK", new string[] { "name" }));

				directAccess.Commit();
			} catch(Exception e) {
				directAccess.Rollback();
				Console.Error.WriteLine(e.Message);
				Console.Error.WriteLine(e.StackTrace);
				Assert.Fail();
			}
			*/
		}
	}
}