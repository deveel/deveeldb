// 
//  Copyright 2010-2018 Deveel
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
//

using System;

using Xunit;

namespace Deveel.Data.Sql.Indexes {
	public static class IndexInfoTests {
		[Fact]
		public static void CreateNewForOneColumn() {
			var indexInfo = new IndexInfo("table1_idx", ObjectName.Parse("sys.table1"), new[] {"col1"});

			Assert.NotNull(indexInfo.IndexName);
			Assert.NotNull(indexInfo.TableName);
			Assert.NotNull(indexInfo.ColumnNames);
			Assert.Equal("sys.table1.table1_idx", indexInfo.FullName.ToString());
			Assert.Single(indexInfo.ColumnNames);
		}
	}
}