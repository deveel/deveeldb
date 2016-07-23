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

namespace Deveel.Data.Deveel.Data {
	[TestFixture]
	public sealed class SelectBlobTests : ContextBasedTest {
		private readonly byte[] testData;

		public SelectBlobTests() {
			var random = new Random();
			testData = new byte[2048];
			for (int i = 0; i < testData.Length; i++) {
				testData[i] = (byte) random.Next();
			}
		}

		protected override bool OnSetUp(string testName, IQuery query) {
			CreateTable(query);
			InsertData(query);
			return true;
		}

		private void CreateTable(IQuery query) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
				new SqlExpression[] { SqlExpression.Constant(tableInfo.TableName.FullName) });
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime());
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());
			tableInfo.AddColumn("data", PrimitiveTypes.Blob(2048));

			query.Session.Access().CreateTable(tableInfo);
			query.Session.Access().AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");
		}

		private void InsertData(IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			var table = query.Access().GetMutableTable(tableName);

			var data = CreateData(query, testData);

			var row = table.NewRow();
			row["id"] = Field.Integer(1);
			row["first_name"] = Field.String("Antonello");
			row["last_name"] = Field.String("Provenzano");
			row["birth_date"] = Field.Date(new SqlDateTime(1980, 06, 04));
			row["active"] = Field.BooleanTrue;
			row["data"] = new Field(PrimitiveTypes.Blob(2048), data);

			table.AddRow(row);
		}

		private static SqlLongString CreateData(IQuery query, byte[] data) {
			var lob = query.Session.CreateLargeObject(2048, true);
			using (var stream = new ObjectStream(lob)) {
				stream.Write(data, 0, data.Length);
				stream.Flush();
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
		public void SelectData() {
			var exp = (SqlQueryExpression)SqlExpression.Parse("SELECT * FROM test_table");
			var result = Query.Select(exp);

			Assert.IsNotNull(result);
			Assert.IsTrue(result.Any());

			var row = result.FirstOrDefault();

			Assert.IsNotNull(row);

			var data = row["data"];

			Assert.IsFalse(Field.IsNullField(data));
			Assert.IsInstanceOf<BinaryType>(data.Type);
			Assert.AreEqual(SqlTypeCode.Blob, data.Type.TypeCode);
			Assert.IsInstanceOf<SqlLongBinary>(data.Value);

			var stream = ((SqlLongBinary)data.Value).GetInput();
			var content = new byte[2048];
			Assert.DoesNotThrow(() => stream.Read(content, 0, content.Length));

			Assert.IsNotEmpty(content);
			Assert.AreEqual(testData, content);
		}

	}
}
