using System;
using System.IO;
using System.Linq;
using System.Text;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;
using Deveel.Data.Store;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public class SelectClobTests : ContextBasedTest {
		const string testBio = "A simple test string that can span several characters, " +
					   "that is trying to be the longest possible, just to prove" +
					   "the capacity of a LONG VARCHAR to handle very long strings. " +
					   "Anyway it is virtually impossible to reach the maximum size " +
					   "of a long object, that is organized in 64k byte pages and " +
					   "spans within the local system without any constraint of size. " +
					   "For sake of memory anyway, the maximum size of the test object " +
					   "is set to just 2048 bytes.";

		protected override bool OnSetUp(string testName, IQuery query) {
			CreateTable(query);
			InsertData(query);
			return true;
		}

		private static void CreateTable(IQuery query) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
				new SqlExpression[] { SqlExpression.Constant(tableInfo.TableName.FullName) });
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime());
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());
			tableInfo.AddColumn("bio", PrimitiveTypes.Clob(2048));

			query.Session.Access().CreateTable(tableInfo);
			query.Session.Access().AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");
		}

		private static void InsertData(IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			var table = query.Access().GetMutableTable(tableName);

			var bio = CreateBio(query, testBio);

			var row = table.NewRow();
			row["id"] = Field.Integer(1);
			row["first_name"] = Field.String("Antonello");
			row["last_name"] = Field.String("Provenzano");
			row["birth_date"] = Field.Date(new SqlDateTime(1980, 06, 04));
			row["active"] = Field.BooleanTrue;
			row["bio"] = new Field(PrimitiveTypes.Clob(2048), bio);

			table.AddRow(row);
		}

		private static SqlLongString CreateBio(IQuery query, string text) {
			var lob = query.Session.CreateLargeObject(2048, true);
			using (var stream = new ObjectStream(lob)) {
				using (var streamWriter = new StreamWriter(stream, Encoding.ASCII)) {
					streamWriter.Write(text);
					streamWriter.Flush();
				}
			}

			lob.Complete();
			return SqlLongString.Ascii(lob);
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			query.Access().DropAllTableConstraints(tableName);
			query.Access().DropObject(DbObjectType.Table, tableName);
			return true;
		}

		[Test]
		public void SelectBio() {
			var exp = (SqlQueryExpression) SqlExpression.Parse("SELECT * FROM test_table");
			var result = Query.Select(exp);

			Assert.IsNotNull(result);
			Assert.IsTrue(result.Any());

			var row = result.FirstOrDefault();

			Assert.IsNotNull(row);

			var bio = row["bio"];

			Assert.IsFalse(Field.IsNullField(bio));
			Assert.IsInstanceOf<StringType>(bio.Type);
			Assert.AreEqual(SqlTypeCode.Clob, bio.Type.TypeCode);
			Assert.IsInstanceOf<SqlLongString>(bio.Value);

			var textReader = ((SqlLongString) bio.Value).GetInput(Encoding.ASCII);
			string text = null;
			Assert.DoesNotThrow(() => text = textReader.ReadToEnd());

			Assert.IsNotNullOrEmpty(text);
			Assert.AreEqual(testBio, text);
		}
	}
}
