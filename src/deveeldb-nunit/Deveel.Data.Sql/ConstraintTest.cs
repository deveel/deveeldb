using System;
using System.Data;
using System.Linq;

using Deveel.Data.Client;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public class ConstraintTest : SqlTestBase {
		private bool HasImportedKey(string tableName, string constraintName) {
			var schema = Connection.GetSchema(DeveelDbMetadataSchemaNames.ImportedKeys, new string[]{"", "APP", tableName});
			return (schema.Rows.Cast<DataRow>().Select(row => row["FK_NAME"])).Any(name => name != null && name.ToString() == constraintName);
		}

		private bool HasExportedKey(string tableName, string constraintName) {
			var schema = Connection.GetSchema(DeveelDbMetadataSchemaNames.ExportedKeys, new string[] { "", "APP", tableName });
			return (schema.Rows.Cast<DataRow>().Select(row => row["FK_NAME"])).Any(name => name != null && name.ToString() == constraintName);			
		}

		[Test]
		public void AddForeignKey() {
			ExecuteNonQuery("ALTER TABLE ListensTo ADD CONSTRAINT ListensTo_People_FK FOREIGN KEY (person_name) REFERENCES Person(name) ON DELETE CASCADE");
			Assert.IsTrue(HasExportedKey("Person", "ListensTo_People_FK"));
			Assert.IsTrue(HasImportedKey("ListensTo", "ListensTo_People_FK"));
		}
	}
}