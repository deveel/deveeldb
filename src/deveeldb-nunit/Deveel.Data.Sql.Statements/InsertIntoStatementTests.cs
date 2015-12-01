// 
//  Copyright 2010-2014 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public sealed class InsertIntoStatementTests : ContextBasedTest {
		private void CreateTestTable() {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			var idColumn = tableInfo.AddColumn("id", PrimitiveTypes.Integer());
			idColumn.DefaultExpression = SqlExpression.FunctionCall("UNIQUE_KEY",
				new SqlExpression[] { SqlExpression.Reference(tableInfo.TableName) });
			tableInfo.AddColumn("first_name", PrimitiveTypes.String());
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("birth_date", PrimitiveTypes.DateTime());
			tableInfo.AddColumn("active", PrimitiveTypes.Boolean());

			Query.CreateTable(tableInfo);
			Query.AddPrimaryKey(tableInfo.TableName, "id", "PK_TEST_TABLE");
		}

		[Test]
		public void ParseValuesInsert_OneRow() {
			const string sql = "INSERT INTO test_table (first_name, last_name, birth_date) "+
				"VALUES ('Antonello', 'Provenzano', TOODATE('1980-06-04'))";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(Query.Context, sql));
			Assert.IsNotNull(statements);

			var statementList = statements.ToList();
			Assert.IsNotEmpty(statementList);
			Assert.AreEqual(1, statementList.Count);

			var statement = statementList[0];
			Assert.IsInstanceOf<InsertStatement>(statement);

			var insertStatement = (InsertStatement) statement;
			Assert.AreEqual("test_table", insertStatement.TableName);
			Assert.AreEqual(3, insertStatement.ColumnNames.Count());
			Assert.AreEqual(1, insertStatement.Values.Count());
		}

		[Test]
		public void ParseValueInsert_MultipleRows() {
			const string sql = "INSERT INTO test_table (first_name, last_name, birth_date) " +
				"VALUES ('Antonello', 'Provenzano', TOODATE('1980-06-04')), " + 
				"('Sebastiano', 'Provenzano', TODATE('1981-08-27'))";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(Query.Context, sql));
			Assert.IsNotNull(statements);

			var statementList = statements.ToList();
			Assert.IsNotEmpty(statementList);
			Assert.AreEqual(1, statementList.Count);

			var statement = statementList[0];
			Assert.IsInstanceOf<InsertStatement>(statement);

			var insertStatement = (InsertStatement)statement;
			Assert.AreEqual("test_table", insertStatement.TableName);
			Assert.AreEqual(3, insertStatement.ColumnNames.Count());
			Assert.AreEqual(2, insertStatement.Values.Count());
		}

		[Test]
		public void ParserSetInsert() {
			const string sql =
				"INSERT INTO test_table SET first_name = 'Antonello', last_name = 'Provenzano', birth_date = TODATE('1980-06-04')";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(Query.Context, sql));
			Assert.IsNotNull(statements);

			var statementList = statements.ToList();
			Assert.IsNotEmpty(statementList);
			Assert.AreEqual(1, statementList.Count);

			var statement = statementList[0];
			Assert.IsInstanceOf<InsertStatement>(statement);

			var insertStatement = (InsertStatement)statement;
			Assert.AreEqual("test_table", insertStatement.TableName);
			Assert.AreEqual(3, insertStatement.ColumnNames.Count());
			Assert.AreEqual(1, insertStatement.Values.Count());
		}

		[Test]
		public void ParseInsertSelect() {
			const string sql = "INSERT INTO test_table FROM (SELECT * FROM table2 WHERE arg1 = 1)";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(Query.Context, sql));
			Assert.IsNotNull(statements);

			var statementList = statements.ToList();
			Assert.IsNotEmpty(statementList);
			Assert.AreEqual(1, statementList.Count);

			var statement = statementList[0];
			Assert.IsInstanceOf<InsertSelectStatement>(statement);
		}
	}
}
