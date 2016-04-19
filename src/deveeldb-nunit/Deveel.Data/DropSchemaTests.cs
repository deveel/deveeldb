﻿// 
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

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DropSchemaTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			query.Access().CreateSchema("test", SchemaTypes.User);
			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			return true;
		}

		[Test]
		public void DropEmptySchema() {
			Query.DropSchema("test");

			var exists = Query.Session.Access().SchemaExists("test");
			Assert.IsFalse(exists);
		}
	}
}
