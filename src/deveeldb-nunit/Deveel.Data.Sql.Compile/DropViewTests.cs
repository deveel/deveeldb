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
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class DropViewTests : SqlCompileTestBase {
		[Test]
		public void OneView() {
			const string sql = "DROP VIEW view1";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
			
			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DropViewStatement>(statement);

			var dropView = (DropViewStatement) statement;

			Assert.IsFalse(dropView.IfExists);
			Assert.AreEqual("view1", dropView.ViewName.FullName);
		}

		[Test]
		public void OneViewIfExists() {
			const string sql = "DROP VIEW IF EXISTS view1";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DropViewStatement>(statement);

			var dropView = (DropViewStatement)statement;

			Assert.IsTrue(dropView.IfExists);
			Assert.AreEqual("view1", dropView.ViewName.FullName);
		}

		[Test]
		public void MultipleViews() {
			const string sql = "DROP VIEW view1, APP.view2";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(2, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<DropViewStatement>(statement);

			var dropView = (DropViewStatement)statement;

			Assert.IsFalse(dropView.IfExists);
			Assert.AreEqual("view1", dropView.ViewName.FullName);
		}
	}
}
