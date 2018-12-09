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
using System.Linq;
using System.Text;

namespace Deveel.Data.Sql.Indexes {
	public sealed class IndexKey : IComparable<IndexKey>, IEquatable<IndexKey> {
		private readonly SqlObject[] values;

		public IndexKey(SqlObject[] values) {
			if (values == null)
				throw new ArgumentNullException(nameof(values));
			if (values.Length == 0)
				throw new ArgumentException("The list of values for a key cannot be empty");

			if (values.Any(x => !x.Type.IsIndexable))
				throw new ArgumentException("One of the value provided is not indexable");

			this.values = values;
		}

		public IndexKey(SqlObject value)
			: this(new[] {value}) {
		}

		public bool IsNull => values.Any(x => x.IsNull);

		public IndexKey NullKey => new IndexKey(values.Select(x => SqlObject.Null).ToArray());

		public int CompareTo(IndexKey other) {
			int c = 0;
			for (int i = 0; i < values.Length; i++) {
				c = (c* i) + values[i].CompareTo(other.values[i]);
			}

			return c;
		}

		public bool Equals(IndexKey other) {
			if (other == null ||
			    values.Length != other.values.Length)
				return false;

			for (int i = 0; i < values.Length; i++) {
				if (!values[i].Equals(other.values[i]))
					return false;
			}

			return true;
		}

		public override string ToString() {
			var sb = new StringBuilder();
			sb.Append("[");
			for (int i = 0; i < values.Length; i++) {
				sb.Append(values[i]);

				if (i < values.Length - 1)
					sb.Append(",");
			}

			sb.Append("]");
			return sb.ToString();
		}
	}
}