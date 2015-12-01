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

using NUnit.Framework;

namespace Deveel.Data.Sql.Parser {
	[TestFixture]
	public static class PlSqlBlockTests {
		[Test]
		public static void ParseCreateSimpleTrigger() {
			const string sql = @"CREATE OR REPLACE TRIGGER test_trigger BEFORE INSERT ON test_table FOR EACH ROW
									DECLARE
										a BOOLEAN NOT NULL;
									BEGIN
										SELECT INTO a FROM table2 WHERE b = 22;
									END";

			SqlParseResult result = null;
			Assert.DoesNotThrow(() => result = SqlParsers.Default.Parse(sql));
			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
		}
	}
}
