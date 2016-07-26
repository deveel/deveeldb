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
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	public class ObjectNameEqualityComparer : IEqualityComparer<ObjectName>, IEqualityComparer {
		public ObjectNameEqualityComparer() 
			: this(false) {
		}

		public ObjectNameEqualityComparer(bool ignoreCase) {
			IgnoreCase = ignoreCase;
		}

		static ObjectNameEqualityComparer() {
			CaseInsensitive = new ObjectNameEqualityComparer(true);
		}

		public bool IgnoreCase { get; private set; }

		public static ObjectNameEqualityComparer CaseInsensitive { get; private set; }

		public bool Equals(ObjectName x, ObjectName y) {
			if (x == null && y == null)
				return true;
			if (x == null || y == null)
				return false;

			return x.Equals(y, IgnoreCase);
		}

		public int GetHashCode(ObjectName obj) {
			return obj.GetHashCode();
		}

		bool IEqualityComparer.Equals(object x, object y) {
			if (x == null && y == null)
				return true;
			if (x == null || y == null)
				return false;

			if (!(x is ObjectName))
				throw new ArgumentException();
			if (!(y is ObjectName))
				throw new ArgumentException();

			return Equals((ObjectName) x, (ObjectName) y);
		}

		int IEqualityComparer.GetHashCode(object obj) {
			return GetHashCode((ObjectName) obj);
		}
	}
}
