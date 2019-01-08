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

using Deveel.Data.Serialization;

using Xunit;

namespace Deveel.Data.Sql.Types {
	public static class SqlArrayTypeTests {
		[Theory]
		[InlineData(4005)]
		[InlineData(92)]
		public static void ConstructArray(int length) {
			var type = new SqlArrayType(length);
			Assert.NotNull(type);
			Assert.Equal(length, type.Length);
		}

		[Theory]
		[InlineData(105, "ARRAY(105)")]
		public static void FormatString(int length, string expected) {
			var type = new SqlArrayType(length);
			Assert.NotNull(type);
			Assert.Equal(length, type.Length);
			Assert.Equal(expected, type.ToString());
		}

		[Theory]
		[InlineData(495, 67, false)]
		[InlineData(123, 123, true)]
		public static void CheckEquality(int length1, int length2, bool expected) {
			var type1 = new SqlArrayType(length1);
			var type2 = new SqlArrayType(length2);

			Assert.Equal(expected, type1.Equals(type2));
		}

		[Theory]
		[InlineData(67)]
		[InlineData(1024)]
		[InlineData(65740000)]
		public static void Serialize(int value) {
			var type = PrimitiveTypes.Array(value);
			var result = BinarySerializeUtil.Serialize(type);

			Assert.Equal(type, result);
		}
	}
}