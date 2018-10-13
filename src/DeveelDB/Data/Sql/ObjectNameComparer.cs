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

namespace Deveel.Data.Sql {
	public sealed class ObjectNameComparer : IEqualityComparer<ObjectName>, IComparer<ObjectName> {
		private readonly bool ignoreCase;

		public ObjectNameComparer(bool ignoreCase) {
			this.ignoreCase = ignoreCase;
		}

		public static ObjectNameComparer IgnoreCase => new ObjectNameComparer(true);

		public static ObjectNameComparer Ordinal => new ObjectNameComparer(false);

		public bool Equals(ObjectName x, ObjectName y) {
			return x.Equals(y, ignoreCase);
		}

		public int GetHashCode(ObjectName obj) {
			return obj.GetHashCode(ignoreCase);
		}

		public int Compare(ObjectName x, ObjectName y) {
			if (x == null && y == null)
				return 0;
			if (x == null)
				return -1;
			if (y == null)
				return 1;

			return x.CompareTo(y, ignoreCase);
		}
	}
}