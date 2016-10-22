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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class InsertTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			if (testName.EndsWith("WithUserType")) {
				var typeName = ObjectName.Parse("APP.type1");
				var typeInfo = new UserTypeInfo(typeName);
				typeInfo.AddMember("a", PrimitiveTypes.String());
				typeInfo.AddMember("b", PrimitiveTypes.Integer());

				query.Access().CreateObject(typeInfo);
			}

			CreateTestTable(query, testName);
			return true;
		}

		protected override void AssertNoErrors(string testName) {
			if (!testName.Equals("NotNullColumnViolation"))
				base.AssertNoErrors(testName);
		}

		private static void CreateTestTable(IQuery query, string testName) {
			var tableName = ObjectName.Parse("APP.test_table");

			query.Access().CreateTable(table => {
				table
					.Named(tableName)
					.WithColumn(column => column.Named("id")
						.HavingType(PrimitiveTypes.Integer())
						.WithDefault(SqlExpression.FunctionCall("UNIQUEKEY",
							new SqlExpression[] {SqlExpression.Constant(tableName.FullName)})))
					.WithColumn("first_name", PrimitiveTypes.String())
					.WithColumn("last_name", PrimitiveTypes.String())
					.WithColumn("birth_date", PrimitiveTypes.DateTime())
					.WithColumn(column => column
						.Named("active")
						.HavingType(PrimitiveTypes.Boolean())
						.NotNull(testName.Equals("NotNullColumnViolation")));

				if (testName.EndsWith("WithLob")) {
					table.WithColumn("bio", PrimitiveTypes.Clob(2048));
				} else if (testName.EndsWith("WithUserType")) {
					var userType = query.Access().ResolveUserType("type1");
					table.WithColumn("user_obj", userType);
				}
			});

			query.Access().AddPrimaryKey(tableName, "id", "PK_TEST_TABLE");
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			var tableName = ObjectName.Parse("APP.test_table");
			query.Access().DropAllTableConstraints(tableName);
			query.Access().DropObject(DbObjectType.Table, tableName);

			if (testName.EndsWith("WithUserType")) {
				var typeName = ObjectName.Parse("APP.test1");
				query.Access().DropType(typeName);
			}

			return true;
		}

		[Test]
		public void TwoValues_NoColumnes() {
			var tableName = ObjectName.Parse("APP.test_table");
			var values = new List<SqlExpression[]> {
				new SqlExpression[] {
					SqlExpression.Constant("Antonello"),
					SqlExpression.Constant("Provenzano"),
					SqlExpression.Constant(true)
				},
				new SqlExpression[] {
					SqlExpression.Constant("Mart"),
					SqlExpression.Constant("Roosmaa"),
					SqlExpression.Constant(false)
				}
			};

			var count = AdminQuery.Insert(tableName, values.ToArray());


			Assert.AreEqual(2, count);

			var table = AdminQuery.Access().GetTable(tableName);

			Assert.IsNotNull(table);
			Assert.AreEqual(2, table.RowCount);
		}

		[Test]
		public void TwoValues() {
			var tableName = ObjectName.Parse("APP.test_table");
			var columns = new[] { "first_name", "last_name", "active" };
			var values = new List<SqlExpression[]> {
				new SqlExpression[] {
					SqlExpression.Constant("Antonello"),
					SqlExpression.Constant("Provenzano"),
					SqlExpression.Constant(true)
				},
				new SqlExpression[] {
					SqlExpression.Constant("Mart"),
					SqlExpression.Constant("Roosmaa"),
					SqlExpression.Constant(false)
				}
			};

			var count = AdminQuery.Insert(tableName, columns, values.ToArray());


			Assert.AreEqual(2, count);

			var table = AdminQuery.Access().GetTable(tableName);

			Assert.IsNotNull(table);
			Assert.AreEqual(2, table.RowCount);
		}

		[Test]
		public void InsertWithUserType() {
			var tableName = ObjectName.Parse("APP.test_table");
			var columns = new[] { "first_name", "last_name", "active", "user_obj" };
			var values = new List<SqlExpression[]> {
				new SqlExpression[] {
					SqlExpression.Constant("Antonello"),
					SqlExpression.Constant("Provenzano"),
					SqlExpression.Constant(true),
					SqlExpression.FunctionCall("type1", new SqlExpression[] {
						SqlExpression.Constant("test1"),
						SqlExpression.Constant(1), 
					})
				},
				new SqlExpression[] {
					SqlExpression.Constant("Mart"),
					SqlExpression.Constant("Roosmaa"),
					SqlExpression.Constant(false),
					SqlExpression.FunctionCall("type1", new SqlExpression[] {
						SqlExpression.Constant("test2"),
						SqlExpression.Constant(3),  
					})
				}
			};

			var count = AdminQuery.Insert(tableName, columns, values.ToArray());


			Assert.AreEqual(2, count);

			var table = AdminQuery.Access().GetTable(tableName);

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

			var count = AdminQuery.Insert(tableName, columns, values.ToArray());

			Assert.AreEqual(1, count);

			var table = AdminQuery.Access().GetTable(tableName);

			Assert.IsNotNull(table);
			Assert.AreEqual(1, table.RowCount);
		}

		private SqlLongString CreateBio(string text) {
			return SqlLongString.Ascii(AdminQuery, text);
		}

		[Test]
		public void NotNullColumnViolation() {
			var expected = Is.InstanceOf<ConstraintViolationException>()
				.And.TypeOf<NotNullColumnViolationException>()
				.And.Property("TableName").EqualTo(ObjectName.Parse("APP.test_table"))
				.And.Property("ColumnName").EqualTo("active");

			Assert.Throws(expected, () => AdminQuery.Insert(new ObjectName("test_table"),
				new[] { "first_name", "last_name", "birth_date", "active" },
				new SqlExpression[] {
				SqlExpression.Constant("Antonello"),
				SqlExpression.Constant("Provenzano"),
				SqlExpression.Constant(new SqlDateTime(1980, 06,  04)),
				SqlExpression.Constant(Field.Null())
			}));
		}
	}
}
