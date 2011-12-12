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

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class VariableTest : TestBase {
		public VariableTest()
			: base(StorageType.Memory) {
		}

		[Test]
		public void DeclareVariables() {
			Connection.CreateCommand("test_var STRING").ExecuteNonQuery();
			Connection.CreateCommand("test_var2 NUMERIC NOT NULL").ExecuteNonQuery();
			Connection.CreateCommand("test_var3 CONSTANT VARCHAR(100) = 'test'").ExecuteNonQuery();
		}

		[Test]
		public void SetVariables() {
			Connection.CreateCommand("SET test_var = 'test1'").ExecuteNonQuery();
			Connection.CreateCommand("SET test_var2 = 245").ExecuteNonQuery();
		}

		[Test]
		public void ShowVariables() {
			object value = Connection.CreateCommand("SELECT :test_var").ExecuteScalar();
			Console.Out.WriteLine("test_var = {0}", value);

			value = Connection.CreateCommand("SELECT :test_var2").ExecuteScalar();
			Console.Out.WriteLine("test_var2 = {0}", value);

			value = Connection.CreateCommand("SELECT :test_var3").ExecuteScalar();
			Console.Out.WriteLine("test_var3 = {0}", value);
		}
	}
}