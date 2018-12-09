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

using Deveel.Data.Sql.Expressions;

using Xunit;

namespace Deveel.Data.Sql.Indexes {
	public static class IndexRangeSetTests {
		[Fact]
		public static void IntersectOnSingleKey() {
			var set = new IndexRangeSet();
			var result = set.Intersect(SqlExpressionType.Equal, new IndexKey(SqlObject.Boolean(true)));

			Assert.NotNull(result);

			var ranges = result.ToArray();

			Assert.Equal(1, ranges.Length);
			Assert.Equal(new IndexKey(SqlObject.Boolean(true)), ranges[0].StartValue);
			Assert.Equal(new IndexKey(SqlObject.Boolean(true)), ranges[0].EndValue);
		}

		[Fact]
		public static void IntersetOnTwoKeys() {
			var set = new IndexRangeSet();
			var result = set.Intersect(SqlExpressionType.LessThan, new IndexKey(SqlObject.Integer(3)));
			result = result.Intersect(SqlExpressionType.GreaterThan, new IndexKey(SqlObject.Integer(12)));

			Assert.NotNull(result);

			var ranges = result.ToArray();

			Assert.Equal(0, ranges.Length);
		}

		[Fact]
		public static void UnionTwoSets() {
			var set1 = new IndexRangeSet();
			set1.Intersect(SqlExpressionType.GreaterThan, new IndexKey(SqlObject.Integer(3)));

			var set2 = new IndexRangeSet();
			set2.Intersect(SqlExpressionType.LessThan, new IndexKey(SqlObject.Integer(12)));

			var result = set1.Union(set2);

			Assert.NotNull(result);

			var ranges = result.ToArray();

			Assert.Equal(2, ranges.Length);
		}
	}
}