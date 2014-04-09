using System;
using System.Data;
using System.Linq;

using Deveel.Data.Client;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public class ConstraintTest : SqlTestBase {
		private FKeyInfo GetImportedKey(string tableName, string constraintName) {
			return GetKeyInfo(tableName, DeveelDbMetadataSchemaNames.ImportedKeys, constraintName);
		}

		private FKeyInfo GetExportedKey(string tableName, string constraintName) {
			return GetKeyInfo(tableName, DeveelDbMetadataSchemaNames.ExportedKeys, constraintName);
		}

		private FKeyInfo GetKeyInfo(string tableName, string collationName, string constraintName) {
			var schema = Connection.GetSchema(collationName, new string[] {"", "APP", tableName});
			if (schema.Rows.Count == 0)
				return null;

			return (schema.Rows.Cast<DataRow>().Where(row => row["FK_NAME"].ToString() == constraintName)
				.Select(FormFKeyInfo))
				.FirstOrDefault();
		}

		private FKeyInfo FormFKeyInfo(DataRow row) {
			return new FKeyInfo {
				Name = row["FK_NAME"].ToString(),
				PrimaryTable = row["PKTABLE_NAME"].ToString(),
				PrimaryColumn = row["PKCOLUMN_NAME"].ToString(),
				ReferenceTable = row["FKTABLE_NAME"].ToString(),
				ReferenceColumn = row["FKCOLUMN_NAME"].ToString(),
				DeleteRule = (FKeyRule)Int32.Parse(row["DELETE_RULE"].ToString()),
				UpdateRule = (FKeyRule)Int32.Parse(row["UPDATE_RULE"].ToString())
			};
		}

		[Test]
		public void AddForeignKey() {
			ExecuteNonQuery(
				"ALTER TABLE ListensTo ADD CONSTRAINT ListensTo_People_FK FOREIGN KEY (person_name) REFERENCES Person(name) ON DELETE CASCADE");

			var exportedKey = GetExportedKey("Person", "ListensTo_People_FK");
			Assert.IsNotNull(exportedKey);
			Assert.AreEqual("Person", exportedKey.PrimaryTable);
			Assert.AreEqual("name", exportedKey.PrimaryColumn);
			Assert.AreEqual("ListensTo", exportedKey.ReferenceTable);
			Assert.AreEqual("person_name", exportedKey.ReferenceColumn);
			Assert.AreEqual(FKeyRule.Cascade, exportedKey.DeleteRule);

			var importedKey = GetImportedKey("ListensTo", "ListensTo_People_FK");
			Assert.IsNotNull(importedKey);
		}

		private class FKeyInfo {
			public string Name { get; set; }

			public string PrimaryTable { get; set; }

			public string ReferenceTable { get; set; }

			public string PrimaryColumn { get; set; }

			public string ReferenceColumn { get; set; }

			public FKeyRule DeleteRule { get; set; }

			public FKeyRule UpdateRule { get; set; }
		}

		private enum FKeyRule {
			Cascade = 1,
			SetNull = 2,
			NoAction = 0,
			SetDefault = 3
		}
	}
}