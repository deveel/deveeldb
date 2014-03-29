using System;

using NUnit.Framework;

namespace Deveel.Data.Control {
	[TestFixture]
	public sealed class DirectAccessTest {
		private DbDirectAccess directAccess;

		[Test(Description = "Creates a new table using the Direct-Access API")]
		public void CreateTable() {
			/*
			TODO:
			try {
				TableName tableName = new TableName("DA_TestTable");
				DataTableInfo tableDef = new DataTableInfo();
				tableDef.TableName = tableName;
				tableDef.AddColumn(DataColumnInfo.CreateStringColumn("name"));
				tableDef.AddColumn(DataColumnInfo.CreateNumericColumn("count"));
				tableDef.AddColumn(DataColumnInfo.CreateBooleanColumn("is_set"));
				directAccess.CreateTable(tableDef, true);

				directAccess.AddConstraint(tableName, DataConstraintInfo.PrimaryKey("DA_TestTable_PK", new string[] { "name" }));

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