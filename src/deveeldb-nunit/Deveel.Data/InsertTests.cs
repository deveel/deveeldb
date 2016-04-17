using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;
using Deveel.Data.Store;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class InsertTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			CreateTestTable(query, testName);
			return true;
		}

		private static void CreateTestTable(IQuery query, string testName) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
				new SqlExpression[] { SqlExpression.Constant(tableInfo.TableName.FullName) });
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime());
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());

			if (testName.EndsWith("WithLob")) {
				tableInfo.AddColumn("bio", PrimitiveTypes.Clob(2048));
			}

			query.Session.Access().CreateTable(tableInfo);
			query.Session.Access().AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			query.Access().DropAllTableConstraints(tableName);
			query.Access().DropObject(DbObjectType.Table, tableName);
			return true;
		}

		[Test]
		public void TwoValues() {
			var tableName = ObjectName.Parse("APP.test_table");
			var columns = new[] { "first_name", "last_name", "active" };
			var values = new List<SqlExpression[]> {
				new[] {
					SqlExpression.Constant("Antonello"),
					SqlExpression.Constant("Provenzano"),
					SqlExpression.Constant(true)
				},
				new [] {
					SqlExpression.Constant("Mart"),
					SqlExpression.Constant("Roosmaa"),
					SqlExpression.Constant(false)
				}
			};

			var count = Query.Insert(tableName, columns, values.ToArray());


			Assert.AreEqual(2, count);

			var table = Query.Access().GetTable(tableName);

			Assert.IsNotNull(table);
			Assert.AreEqual(2, table.RowCount);
		}

		[Test]
		public void OneValueWithLob() {
			const string testBio = "A simple test string that can span several characters, " +
			                       "that is trying to be the longest possible, just to prove" +
			                       "the capacity of a LONG VARCHAR to handle very long strings. " +
			                       "Anyway it is virtually impossible to reach the maximum size " +
			                       "of a long object, that is organized in 64k byte pages and " +
			                       "spans within the local system without any constraint of size. " +
			                       "For sake of memory anyway, the maximum size of the test object " +
			                       "is set to just 2048 bytes.";

			var tableName = ObjectName.Parse("APP.test_table");
			var columns = new[] { "first_name", "last_name", "active", "bio" };
			var bio = CreateBio(testBio);

			var values = new List<SqlExpression[]> {
				new[] {
					SqlExpression.Constant("Antonello"),
					SqlExpression.Constant("Provenzano"),
					SqlExpression.Constant(true),
					SqlExpression.Constant(bio)
				},
			};

			var count = Query.Insert(tableName, columns, values.ToArray());

			Assert.AreEqual(1, count);

			var table = Query.Access().GetTable(tableName);

			Assert.IsNotNull(table);
			Assert.AreEqual(1, table.RowCount);
		}

		private SqlLongString CreateBio(string text) {
			var lob = Session.CreateLargeObject(2048, true);
			using (var stream = new ObjectStream(lob)) {
				using (var streamWriter = new StreamWriter(stream, Encoding.ASCII)) {
					streamWriter.Write(text);
					streamWriter.Flush();
				}
			}

			lob.Complete();
			return SqlLongString.Ascii(lob);
		}
	}
}
