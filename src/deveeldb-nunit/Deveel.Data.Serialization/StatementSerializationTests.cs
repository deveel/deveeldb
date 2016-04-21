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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Serialization {
	[TestFixture]
	public class StatementSerializationTests : SerializationTestBase {
		[Test]
		public void SerializeSelect() {
			var expression = (SqlQueryExpression)SqlExpression.Parse("SELECT * FROM table1 WHERE a = 1");
			var statement = new SelectStatement(expression);

			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.IsInstanceOf<SelectStatement>(deserialized);

				Assert.IsNotNull(deserialized.QueryExpression);
			});
		}

		[Test]
		public void Goto() {
			var statement = new GoToStatement("test");

			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.AreEqual(serialized.Label, deserialized.Label);
			});
		}

		[Test]
		public void Exit() {
			var statement = new ExitStatement();
			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.IsNull(deserialized.Label);
				Assert.IsNull(serialized.WhenExpression);
			});
		}

		[Test]
		public void ExitLabel() {
			var statement = new ExitStatement("test");
			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.IsNotNull(deserialized.Label);
				Assert.AreEqual(serialized.Label, deserialized.Label);
				Assert.IsNull(deserialized.WhenExpression);
			});
		}

		[Test]
		public void ExitWhen() {
			var statement = new ExitStatement(SqlExpression.Constant(true));
			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.IsNull(deserialized.Label);
				Assert.IsNotNull(deserialized.WhenExpression);
				Assert.IsInstanceOf<SqlConstantExpression>(deserialized.WhenExpression);
			});
		}

		[Test]
		public void Continue() {
			var statement = new ContinueStatement();
			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.IsNull(deserialized.Label);
				Assert.IsNull(serialized.WhenExpression);
			});
		}

		[Test]
		public void ContinueLabel() {
			var statement = new ContinueStatement("test");
			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.IsNotNull(deserialized.Label);
				Assert.AreEqual(serialized.Label, deserialized.Label);
				Assert.IsNull(deserialized.WhenExpression);
			});
		}

		[Test]
		public void ContinueWhen() {
			var statement = new ContinueStatement(SqlExpression.Constant(true));
			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.IsNull(deserialized.Label);
				Assert.IsNotNull(deserialized.WhenExpression);
				Assert.IsInstanceOf<SqlConstantExpression>(deserialized.WhenExpression);
			});
		}

		[Test]
		public void GoTo() {
			var statement = new GoToStatement("block1");

			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.IsNotNull(deserialized.Label);
				Assert.AreEqual("block1", deserialized.Label);
			});
		}

		[Test]
		public void CallNoArgs() {
			var name = ObjectName.Parse("APP.proc1");
			var statement = new CallStatement(name);

			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.IsNotNull(deserialized.ProcedureName);
				Assert.IsEmpty(deserialized.Arguments);

				Assert.AreEqual("APP", deserialized.ProcedureName.ParentName);
				Assert.AreEqual("proc1", deserialized.ProcedureName.Name);
			});
		}

		[Test]
		public void CallWithArgs() {
			var name = ObjectName.Parse("APP.proc1");
			var args = new SqlExpression[] {
				SqlExpression.Constant(32),
				SqlExpression.Reference(new ObjectName("a"))
			};

			var statement = new CallStatement(name, args);

			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.IsNotNull(deserialized.ProcedureName);
				Assert.IsNotNull(deserialized.Arguments);
				Assert.IsNotEmpty(deserialized.Arguments);

				Assert.AreEqual("APP", deserialized.ProcedureName.ParentName);
				Assert.AreEqual("proc1", deserialized.ProcedureName.Name);
			});
		}


		[Test]
		public void DeclareVariable() {
			var statement = new DeclareVariableStatement("a", PrimitiveTypes.String());

			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.IsNotNull(deserialized.VariableName);
				Assert.IsNotNull(deserialized.VariableType);

				Assert.AreEqual("a", deserialized.VariableName);
				Assert.IsInstanceOf<StringType>(deserialized.VariableType);
			});
		}

		[Test]
		public void DeclareCursor() {
			var query = (SqlQueryExpression) SqlExpression.Parse("SELECT * FROM table1, table2");
			var statement = new DeclareCursorStatement("c1", query);

			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.AreEqual("c1", deserialized.CursorName);
				Assert.IsNotNull(deserialized.QueryExpression);
			});
		}

		[Test]
		public void FetchWithNoOffset() {
			var statement = new FetchStatement("c1", FetchDirection.Next);

			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.AreEqual("c1", deserialized.CursorName);
				Assert.AreEqual(FetchDirection.Next, statement.Direction);
			});
		}

		[Test]
		public void FetchWithOffset() {
			var statement = new FetchStatement("c1", FetchDirection.Absolute, SqlExpression.Constant(33));

			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.AreEqual("c1", deserialized.CursorName);
				Assert.AreEqual(FetchDirection.Absolute, statement.Direction);
				Assert.IsNotNull(deserialized.OffsetExpression);
			});
		}

		[Test]
		public void ForLoop() {
			var statement = new ForLoopStatement("i", SqlExpression.Constant(22), SqlExpression.Constant(56));
			statement.Statements.Add(new CallStatement(new ObjectName("proc1")));

			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.IsNotNull(deserialized.IndexName);
				Assert.AreEqual("i", deserialized.IndexName);
				Assert.IsNotNull(deserialized.LowerBound);
				Assert.IsNotNull(deserialized.UpperBound);
				Assert.IsNotNull(deserialized.Statements);
				Assert.IsNotEmpty(deserialized.Statements);
			});
		}

		[Test]
		public void InsertWithColumns() {
			var columnNames = new string[] {"id", "text"};
			var values = new SqlExpression[][] {
				new SqlExpression[] {
					SqlExpression.Constant(22),
					SqlExpression.Constant("test"),
				},
			};

			var tableName = ObjectName.Parse("APP.test_table");
			var statement = new InsertStatement(tableName, columnNames, values);

			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.IsNotNull(deserialized.TableName);
				Assert.IsNotNull(deserialized.ColumnNames);
				Assert.IsNotEmpty(deserialized.ColumnNames);
				Assert.IsNotNull(deserialized.Values);
				Assert.IsNotEmpty(deserialized.Values);
			});
		}

		[Test]
		public void OpenWithNoArguments() {
			var statement = new OpenStatement("c1");

			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.AreEqual("c1", deserialized.CursorName);
				Assert.IsEmpty(deserialized.Arguments);
			});
		}

		[Test]
		public void Close() {
			var statement = new CloseStatement("c1");

			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.AreEqual("c1", deserialized.CursorName);
			});
		}

		[Test]
		public void EmptyReturn() {
			var statement = new ReturnStatement();

			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.IsNull(deserialized.ReturnExpression);
			});
		}

		[Test]
		public void ReturnValue() {
			var statement = new ReturnStatement(SqlExpression.Constant(20));

			SerializeAndAssert(statement, (serialized, deserialized) => {
				Assert.IsNotNull(deserialized);
				Assert.IsNotNull(deserialized.ReturnExpression);
				Assert.IsInstanceOf<SqlConstantExpression>(deserialized.ReturnExpression);
			});
		}
	}
}
