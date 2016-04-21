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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class InsertIntoTests : SchemaCompileTests {
		[Test]
		public void ValuesInsert_OneRow() {
			const string sql = "INSERT INTO test_table (first_name, last_name, birth_date) " +
			                   "VALUES ('Antonello', 'Provenzano', TODATE('1980-06-04'))";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);
			Assert.IsInstanceOf<InsertStatement>(statement);

			var insertStatement = (InsertStatement) statement;
			Assert.AreEqual("test_table", insertStatement.TableName.FullName);
			Assert.AreEqual(3, insertStatement.ColumnNames.Count());
			Assert.AreEqual(1, insertStatement.Values.Count());

			var firstRow = insertStatement.Values.ElementAt(0);
			Assert.AreEqual(3, firstRow.Length);
			Assert.IsInstanceOf<SqlFunctionCallExpression>(firstRow[2]);
		}

		[Test]
		public void ValueInsert_MultipleRows() {
			const string sql = "INSERT INTO test_table (first_name, last_name, birth_date) " +
			                   "VALUES ('Antonello', 'Provenzano', TODATE('1980-06-04')), " +
			                   "('Sebastiano', 'Provenzano', TODATE('1981-08-27'))";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);
			Assert.IsInstanceOf<InsertStatement>(statement);

			var insertStatement = (InsertStatement) statement;
			Assert.AreEqual("test_table", insertStatement.TableName.FullName);
			Assert.AreEqual(3, insertStatement.ColumnNames.Count());
			Assert.AreEqual(2, insertStatement.Values.Count());
		}

		[Test]
		public void SetInsert() {
			const string sql =
				"INSERT INTO test_table SET first_name = 'Antonello', last_name = 'Provenzano', birth_date = TODATE('1980-06-04')";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);
			Assert.IsInstanceOf<InsertStatement>(statement);

			var insertStatement = (InsertStatement) statement;
			Assert.AreEqual("test_table", insertStatement.TableName.FullName);
			Assert.AreEqual(3, insertStatement.ColumnNames.Count());
			Assert.AreEqual(1, insertStatement.Values.Count());
		}

		[Test]
		public void InsertSelect() {
			const string sql = "INSERT INTO test_table SELECT * FROM table2 WHERE arg1 = 1";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);
			Assert.IsInstanceOf<InsertSelectStatement>(statement);
		}
	}
}