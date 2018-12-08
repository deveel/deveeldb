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

using Deveel.Data.Sql.Types;

using Xunit;

namespace Deveel.Data.Sql.Tables {
	public sealed class ColumnInfoTests {
		[Theory]
		[InlineData("a", "VARCHAR", true)]
		[InlineData("bin", "VARBINARY", false)]
		public static void NewSimpleColumn(string name, string type, bool indexable) {
			var column = new ColumnInfo(name, PrimitiveTypes.Type(type));

			Assert.Equal(name, column.ColumnName);
			Assert.Equal(name, column.FullName.ToString());
			Assert.NotNull(column.ColumnType);
			Assert.Equal(type, column.ColumnType.ToSqlString());
			Assert.Equal(indexable, column.IsIndexable);
			Assert.Null(column.TableInfo);
			Assert.Null(column.DefaultValue);
			Assert.False(column.HasDefault);
		}
	}
}