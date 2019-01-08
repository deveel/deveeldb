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
	public static class IndexKeyTests {
		[Theory]
		[InlineData(3L, 3L, true)]
		[InlineData(45, 32, false)]
		public static void SingleValueKeyEqual(object value1, object value2, bool expected) {
			var key1 = new IndexKey(SqlObject.New(SqlValueUtil.FromObject(value1)));
			var key2 = new IndexKey(SqlObject.New(SqlValueUtil.FromObject(value2)));

			Assert.Equal(expected, key1.Equals(key2));
		}

		[Theory]
		[InlineData(748, true, 903, true, false)]
		[InlineData(1920, 11, 1920, 11, true)]
		public static void MultiValueKeyEqual(object value1a, object value1b, object value2a, object value2b, bool expected) {
			var key1 = new IndexKey(new [] {
				SqlObject.New(SqlValueUtil.FromObject(value1a)),
				SqlObject.New(SqlValueUtil.FromObject(value1b))
			});
			var key2 = new IndexKey(new[] {
				SqlObject.New(SqlValueUtil.FromObject(value2a)),
				SqlObject.New(SqlValueUtil.FromObject(value2b))
			});

			Assert.Equal(expected, key1.Equals(key2));
		}

		[Theory]
		[InlineData(748, true)]
		public static void MultiValueKeyEqualToNull(object value1, object value2) {
			var key1 = new IndexKey(new[] {
				SqlObject.New(SqlValueUtil.FromObject(value1)),
				SqlObject.New(SqlValueUtil.FromObject(value2))
			});
			var key2 = key1.NullKey;

			Assert.NotEqual(key1, key2);
			Assert.True(key2.IsNull);
		}


		[Theory]
		[InlineData(657, 43, 1)]
		public static void CompareSingleValue(object value1, object value2, int expetced) {
			var key1 = new IndexKey(SqlObject.New(SqlValueUtil.FromObject(value1)));
			var key2 = new IndexKey(SqlObject.New(SqlValueUtil.FromObject(value2)));

			Assert.Equal(expetced, key1.CompareTo(key2));
		}

		[Theory]
		[InlineData(657, 43, 657, 11, 1)]
		public static void CompareMultipleValue(object value1a, object value1b, object value2a, object value2b, int expetced) {
			var key1 = new IndexKey(new[] {
				SqlObject.New(SqlValueUtil.FromObject(value1a)),
				SqlObject.New(SqlValueUtil.FromObject(value1b))
			});
			var key2 = new IndexKey(new[] {
				SqlObject.New(SqlValueUtil.FromObject(value2a)),
				SqlObject.New(SqlValueUtil.FromObject(value2b))
			});

			Assert.Equal(expetced, key1.CompareTo(key2));
		}
	}
}