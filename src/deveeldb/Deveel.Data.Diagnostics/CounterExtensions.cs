// 
//  Copyright 2010-2016 Deveel
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
using System.Globalization;

using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Diagnostics {
	public static class CounterExtensions {
		public static T ValueAs<T>(this ICounter counter) {
			if (counter.Value == null)
				return default(T);

			if (counter.Value is T)
				return (T) counter.Value;

			return (T) Convert.ChangeType(counter.Value, typeof(T), CultureInfo.InvariantCulture);
		}

		public static SqlNumber ValueAsNumber(this ICounter counter) {
			if (counter.Value == null)
				return SqlNumber.Null;

			if (counter.Value is byte ||
				counter.Value is int)
				return new SqlNumber((int)counter.Value);
			if (counter.Value is long)
				return new SqlNumber((long)counter.Value);
			if (counter.Value is float ||
				counter.Value is double)
				return new SqlNumber((double)counter.Value);

			throw new InvalidOperationException();
		}
	}
}
