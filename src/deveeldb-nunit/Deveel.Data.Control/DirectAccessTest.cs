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
				DataTableInfo tableDef = new DataTableInfo();
				tableDef.TableName = tableName;
				tableDef.AddColumn(DataTableColumnInfo.CreateStringColumn("name"));
				tableDef.AddColumn(DataTableColumnInfo.CreateNumericColumn("count"));
				tableDef.AddColumn(DataTableColumnInfo.CreateBooleanColumn("is_set"));
				directAccess.CreateTable(tableDef, true);

				directAccess.AddConstraint(tableName, DataTableConstraintInfo.PrimaryKey("DA_TestTable_PK", new string[] { "name" }));

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