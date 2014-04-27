// 
//  Copyright 2011 Deveel
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

using Deveel.Data.DbSystem;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public sealed class InsertTest : TestBase {
		private void CreateTable() {
			ExecuteNonQuery("CREATE TABLE Person (id IDENTITY, first_name VARCHAR(100), last_name VARCHAR(100))");
		}

		protected override void OnTestSetUp() {
			Connection.AutoCommit = true;

			CreateTable();

			base.OnTestSetUp();
		}

		protected override void OnTestTearDown() {
			ExecuteNonQuery("DROP TABLE Person");

			base.OnTestTearDown();
		}

		[Test]
		public void SimpleInsert() {
			ExecuteNonQuery("INSERT INTO Person (first_name, last_name) VALUES ('Antonello', 'Provenzano')");

			IDatabaseConnection connection = CreateDatabaseConnection();
			Table table = connection.GetTable("Person");

			Assert.AreEqual(1, table.RowCount);
			Assert.AreEqual("Antonello", table.GetFirstCell("first_name").ToString());
			Assert.AreEqual("Provenzano", table.GetFirstCell("last_name").ToString());
			Assert.AreEqual(1, table.GetFirstCell("id").ToBigNumber().ToInt32());
		}

		[Test]
		public void InsertWithoutColumns() {
			ExecuteNonQuery("INSERT INTO Person VALUES (22, 'Antonello','Provenzano')");
		}

		[Test]
		public void InsertMultimpleValues() {
			ExecuteNonQuery("INSERT INTO Person (first_name, last_name) VALUES ('Antonello', 'Provenzano'), ('Sebastiano', 'Provenzano'), ('Mart', 'Roosmaa')");

			IDatabaseConnection connection = CreateDatabaseConnection();
			Table table = connection.GetTable("Person");

			Assert.AreEqual(3, table.RowCount);
		}

		[Test]
		public void InsertFromSelect() {
			ExecuteNonQuery("INSERT INTO Person (first_name, last_name) VALUES ('Antonello', 'Provenzano'), ('Sebastiano', 'Provenzano'), ('Mart', 'Roosmaa')");
			ExecuteNonQuery("CREATE TABLE FirstNames (name VARCHAR(100))");
			ExecuteNonQuery("INSERT INTO FirstNames (name) SELECT first_name FROM Person");

			IDatabaseConnection connection = CreateDatabaseConnection();
			Table table = connection.GetTable("FirstNames");

			Assert.AreEqual(3, table.RowCount);
		}

		[Test]
		public void InsertFromAssignment() {
			ExecuteNonQuery("INSERT INTO Person SET first_name = 'Antonello', last_name = 'Provenzano'");

			IDatabaseConnection connection = CreateDatabaseConnection();
			Table table = connection.GetTable("Person");

			Assert.AreEqual(1, table.RowCount);
			Assert.AreEqual("Antonello", table.GetFirstCell("first_name").ToString());
			Assert.AreEqual("Provenzano", table.GetFirstCell("last_name").ToString());
			Assert.AreEqual(1, table.GetFirstCell("id").ToBigNumber().ToInt32());
		}
	}
}