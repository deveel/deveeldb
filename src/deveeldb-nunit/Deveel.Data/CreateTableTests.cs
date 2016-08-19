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

using Deveel.Data.Index;
using Deveel.Data.Mapping;
using Deveel.Data.Security;
using Deveel.Data.Services;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

using Moq;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture(StorageType.InMemory)]
	[TestFixture(StorageType.SingleFile)]
	[TestFixture(StorageType.JournaledFile)]
	public sealed class CreateTableTests : ContextBasedTest {
		public CreateTableTests(StorageType storageType)
			: base(storageType) {
		}

		private ObjectName tableName;

		protected override bool CreateTestUser {
			get { return true; }
		}

		private static void CreateUserType(IQuery query) {
			var typeName = ObjectName.Parse("APP.test_type");
			var typeInfo = new UserTypeInfo(typeName);
			typeInfo.AddMember("a", PrimitiveTypes.String());
			typeInfo.AddMember("b", PrimitiveTypes.Integer());

			query.Access().CreateType(typeInfo);
		}

		protected override bool OnSetUp(string testName, IQuery query) {
			if (testName == "WithUserType") {
				CreateUserType(query);
				return true;
			}

			return base.OnSetUp(testName, query);
		}

		protected override void RegisterServices(ServiceContainer container) {
			var mock = new Mock<IIndexFactory>();
			mock.Setup(obj => obj.CreateIndex(It.IsAny<ColumnIndexContext>()))
				.Returns<ColumnIndexContext>(context => {
					var cmock = new Mock<ColumnIndex>(context.Table, context.ColumnOffset);
					return cmock.Object;
				});
			mock.Setup(obj => obj.HandlesIndexType(It.IsAny<string>()))
				.Returns(true);

			container.Bind<IIndexFactory>()
				.ToInstance(mock.Object);
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			query.Access().DropAllTableConstraints(tableName);
			query.Access().DropObject(DbObjectType.Table, tableName);

			if (testName == "WithUserType") {
				var typeName = ObjectName.Parse("APP.test_type");
				query.Access().DropType(typeName);
			}

			return true;
		}

		[Test]
		public void SimpleCreate() {
			tableName = ObjectName.Parse("APP.test");
			var columns = new SqlTableColumn[] {
				new SqlTableColumn("id", PrimitiveTypes.Integer()),
				new SqlTableColumn("name", PrimitiveTypes.VarChar()),
			};

			AdminQuery.CreateTable(tableName, columns);

			var table = AdminQuery.Access().GetTable(tableName);

			Assert.IsNotNull(table);
			Assert.AreEqual(2, table.TableInfo.ColumnCount);

			// TODO: Assert it exists and has the structure desired...
		}

		[Test]
		public void SimpleCreateFromUnauthorized() {
			tableName = ObjectName.Parse("APP.test");
			var columns = new SqlTableColumn[] {
				new SqlTableColumn("id", PrimitiveTypes.Integer()),
				new SqlTableColumn("name", PrimitiveTypes.VarChar()),
			};

			var expected = Is.InstanceOf<SecurityException>();

			Assert.Throws(expected, () => UserQuery.CreateTable(tableName, columns));
		}

		[Test]
		public void WithIndexedColumn_InsertSearch() {
			tableName = ObjectName.Parse("APP.test");
			var columns = new SqlTableColumn[] {
				new SqlTableColumn("id", PrimitiveTypes.Integer()),
				new SqlTableColumn("name", PrimitiveTypes.VarChar()) {
					IndexType = DefaultIndexTypes.InsertSearch
				},
			};

			AdminQuery.CreateTable(tableName, columns);

			var table = AdminQuery.Access().GetTable(tableName);

			Assert.IsNotNull(table);
			Assert.AreEqual(2, table.TableInfo.ColumnCount);
		}

		[Test]
		public void WithIndexedColumn_CustomIndex() {
			tableName = ObjectName.Parse("APP.test");

			var query = CreateQuery(CreateAdminSession(Database));

			var columns = new SqlTableColumn[] {
				new SqlTableColumn("id", PrimitiveTypes.Integer()),
				new SqlTableColumn("name", PrimitiveTypes.VarChar()) {
					IndexType = "foo"
				},
			};

			query.CreateTable(tableName, columns);
			query.Commit();

			query = CreateQuery(CreateAdminSession(Database));
			var table = query.Access().GetTable(tableName);

			Assert.IsNotNull(table);
			Assert.AreEqual(2, table.TableInfo.ColumnCount);
		}

		[Test]
		public void WithColumnDefault() {
			tableName = ObjectName.Parse("APP.test");
			var columns = new SqlTableColumn[] {
				new SqlTableColumn("id", PrimitiveTypes.Integer()),
				new SqlTableColumn("name", PrimitiveTypes.VarChar()) {
					DefaultExpression = SqlExpression.Parse("((67 * 90) + 22)")
				},
				new SqlTableColumn("date", PrimitiveTypes.TimeStamp()) {
					DefaultExpression = SqlExpression.Parse("GetDate()")
				}
			};

			AdminQuery.CreateTable(tableName, columns);

			// TODO: Assert it exists and has the structure desired...
		}

		[Test]
		public void WithColumnIndex() {
			tableName = ObjectName.Parse("APP.test");
			var columns = new SqlTableColumn[] {
				new SqlTableColumn("id", PrimitiveTypes.Integer()),
				new SqlTableColumn("name", PrimitiveTypes.VarChar()) {
					IndexType = "BLIST"
				},
				new SqlTableColumn("date", PrimitiveTypes.TimeStamp()) {
					DefaultExpression = SqlExpression.Parse("GetDate()")
				}
			};

			AdminQuery.CreateTable(tableName, columns);
		}

		[Test]
		public void WithUserType() {
			tableName = ObjectName.Parse("APP.test");
			var columns = new[] {
				new SqlTableColumn("id", PrimitiveTypes.Integer()),
				new SqlTableColumn("t", AdminQuery.Context.ResolveType("APP.test_type")),  
			};

			AdminQuery.CreateTable(tableName, columns);
		}
	}
}
