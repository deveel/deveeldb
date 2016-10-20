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
	public sealed class Counter : ICounter {
		internal Counter(string name, object value) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			Name = name;
			Value = value;
		}

		public string Name { get; private set; }

		public object Value { get; private set; }

		internal void Increment() {
			object value = Value;
			if (value == null) {
				value = 1L;
			} else {
				if (value is long) {
					value = ((long)value) + 1;
				} else if (value is int) {
					value = (int)value + 1;
				} else if (value is double) {
					value = (double)value + 1;
				} else {
					throw new InvalidOperationException(String.Format("The value for '{0}' is not a numeric.", Name));
				}
			}

			Value = value;
		}
	}
}
