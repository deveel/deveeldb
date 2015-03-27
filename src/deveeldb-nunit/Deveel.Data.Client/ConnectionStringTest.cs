// 
//  Copyright 2012 Deveel
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

using Deveel.Data.Client;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class ConnectionStringTest {
		[Test]
		public void StandardParseFromString() {
			const string connString = "Host=Heap;UserID=SA;Password=123456;Database=testdb";
			DeveelDbConnectionStringBuilder connectionString = new DeveelDbConnectionStringBuilder(connString);
			Assert.AreEqual("Heap", connectionString.Host);
			Assert.AreEqual("SA", connectionString.UserName);
			Assert.AreEqual("123456", connectionString.Password);
			Assert.AreEqual("testdb", connectionString.Database);
		}

		[Test]
		public void NonStandardParseFromString() {
			const string connString = "DataSource=Heap;UserName=SA;Password=123456;Database=testdb;BootOrCreate=true";
			DeveelDbConnectionStringBuilder connectionString = new DeveelDbConnectionStringBuilder(connString);
			Assert.AreEqual("Heap", connectionString.Host);
			Assert.AreEqual("SA", connectionString.UserName);
			Assert.AreEqual("123456", connectionString.Password);
			Assert.AreEqual("testdb", connectionString.Database);
			Assert.AreEqual(true, connectionString.BootOrCreate);
		}
	}
}