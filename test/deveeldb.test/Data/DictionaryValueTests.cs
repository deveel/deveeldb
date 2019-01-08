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
using System.Collections.Generic;
using System.Linq;

using Xunit;

namespace Deveel.Data {
	public static class DictionaryValueTests {
		[Fact]
		public static void GetNullKey() {
			var dictionary = new Dictionary<string, object> {
				{ "key1", "Hello!" }
			};

			Assert.Throws<ArgumentNullException>(() => dictionary.GetValue<string>(null));
		}

		[Fact]
		public static void GetFromNullDictionary() {
			IDictionary<string, object> dictionary = null;

			var value = dictionary.GetValue<string>("key");
			Assert.Null(value);
		}

		[Fact]
		public static void GetConvertibleValue() {
			var dictionary = new Dictionary<string, object> {
				{ "key1", "Hello!" },
				{ "key2", 456 }
			};

			var value = dictionary.GetValue<double>("key2");
			Assert.Equal((double)456, value);
		}

		[Fact]
		public static void GetNotFoundValue() {
			var dictionary = new Dictionary<string, object> {
				{ "key1", "Hello!" },
				{ "key2", 456 }
			};

			var value = dictionary.GetValue<double?>("key3");
			Assert.Null(value);
		}

		[Fact]
		public static void GetFromEnumerable() {
			var dictionary = new Dictionary<string, object> {
				{ "key1", "Hello!" },
				{ "key2", 456 }
			}.AsEnumerable();

			var value = dictionary.GetValue<string>("key1");
			Assert.NotNull(value);
			Assert.Equal("Hello!", value);
		}

		[Fact]
		public static void GetNullableConvert() {
			var dictionary = new Dictionary<string, object> {
				{ "key1", "Hello!" },
				{ "key2", 456 }
			};

			var value = dictionary.GetValue<double?>("key2");
			Assert.NotNull(value);
			Assert.Equal(456, value);
		}
	}
}