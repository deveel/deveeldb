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
	public class CreateTableStatementTests : ContextBasedTest {
		[Test]
		public void SimpleCreate() {
			var tableName = ObjectName.Parse("APP.test");
			var columns = new SqlTableColumn[] {
				new SqlTableColumn("id", PrimitiveTypes.Integer()),
				new SqlTableColumn("name", PrimitiveTypes.VarChar()),  
			};
			var statement = new CreateTableStatement(tableName, columns);

			ITable result = null;
			Assert.DoesNotThrow(() => result = statement.Execute(Query));
			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
		}

		[Test]
		public void WithColumnDefault() {
			var tableName = ObjectName.Parse("APP.test");
			var columns = new SqlTableColumn[] {
				new SqlTableColumn("id", PrimitiveTypes.Integer()),
				new SqlTableColumn("name", PrimitiveTypes.VarChar()) {
					DefaultExpression = SqlExpression.Parse("((67 * 90) + 22)")
				},
				new SqlTableColumn("date", PrimitiveTypes.TimeStamp()) {
					DefaultExpression = SqlExpression.Parse("GetDate()")
				}
			};

			var statement = new CreateTableStatement(tableName, columns);

			ITable result = null;
			Assert.DoesNotThrow(() => result = statement.Execute(Query));
			Assert.IsNotNull(result);
			Assert.AreEqual(1, result.RowCount);
		}

		[Test]
		public void ParseWithColumnDefault() {
			const string sql = "CREATE TABLE test (id INT, name VARCHAR DEFAULT (67 * 90)+22, date TIMESTAMP DEFAULT GetDate())";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var list = statements.ToList();

			Assert.AreEqual(1, list.Count);

			var statement = list[0];

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateTableStatement>(statement);

			var createTable = (CreateTableStatement)statement;
			Assert.AreEqual(3, createTable.Columns.Count);

			var columns = createTable.Columns;

			Assert.AreEqual("id", columns[0].ColumnName);
			Assert.AreEqual(SqlTypeCode.Integer, columns[0].ColumnType.TypeCode);

			Assert.AreEqual("name", columns[1].ColumnName);
			Assert.IsInstanceOf<StringType>(columns[1].ColumnType);
			Assert.IsTrue(columns[1].HasDefaultExpression);
			Assert.IsInstanceOf<SqlBinaryExpression>(columns[1].DefaultExpression);

			Assert.AreEqual("date", columns[2].ColumnName);
			Assert.IsInstanceOf<DateType>(columns[2].ColumnType);
			Assert.IsTrue(columns[1].HasDefaultExpression);
			Assert.IsInstanceOf<SqlFunctionCallExpression>(columns[2].DefaultExpression);
		}

		[Test]
		public void ParseWithColumnIdentity() {
			const string sql = "CREATE TABLE test (id INT IDENTITY, name VARCHAR NOT NULL)";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var list = statements.ToList();

			Assert.AreEqual(1, list.Count);

			var statement = list[0];

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<CreateTableStatement>(statement);

			var createTable = (CreateTableStatement)statement;
			Assert.AreEqual(2, createTable.Columns.Count);

			var columns = createTable.Columns;

			Assert.AreEqual("id", columns[0].ColumnName);
			Assert.AreEqual(SqlTypeCode.Integer, columns[0].ColumnType.TypeCode);
			Assert.IsNotNull(columns[0].DefaultExpression);
			Assert.IsInstanceOf<SqlFunctionCallExpression>(columns[0].DefaultExpression);

			Assert.AreEqual("name", columns[1].ColumnName);
			Assert.IsInstanceOf<StringType>(columns[1].ColumnType);
			Assert.IsTrue(columns[1].IsNotNull);
		}

		[Test]
		public void ParseWithColumnConstraints() {
			const string sql = "CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR NOT NULL)";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var list = statements.ToList();

			Assert.AreEqual(2, list.Count);
		}

		[Test]
		public void ParseWithColumnAndTableConstraints() {
			const string sql = "CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR NOT NULL, CONSTRAINT uk_test UNIQUE(name))";

			IEnumerable<SqlStatement> statements = null;
			Assert.DoesNotThrow(() => statements = SqlStatement.Parse(sql));
			Assert.IsNotNull(statements);

			var list = statements.ToList();

			Assert.AreEqual(3, list.Count);
		}
	}
}
