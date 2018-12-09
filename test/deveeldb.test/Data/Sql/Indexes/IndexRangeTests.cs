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
	public static class IndexRangeTests {
		[Fact]
		public static void FullRangeEqual() {
			var fullRange1 = IndexRange.FullRange;
			var fullRange2 = IndexRange.FullRange;

			Assert.Equal(fullRange1, fullRange2);
		}

		[Fact]
		public static void IndexKeyNotEqualsFirstInSet() {
			var firstInSet = IndexRange.FirstInSet;
			var key = new IndexKey(new []{SqlObject.BigInt(33), SqlObject.Double(54) });

			Assert.NotEqual(firstInSet, key);
		}
	}
}