// 
//  Copyright 2010-2014 Deveel
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

using System;
using System.Globalization;

namespace Deveel.Data.Text {
	public sealed class SystemCollatorFactory : ICollatorFactory {
		#region ICollatorFactory Members
		public ICollator CreateCollator(CultureInfo locale, CollationStrength strength, CollationDecomposition decomposition) {
			return new SystemCollator(locale);
		}
		#endregion

		#region SystemCollator
		class SystemCollator : ICollator {
			#region .ctor
			public SystemCollator(CultureInfo locale) {
				this.locale = locale;
			}
			#endregion

			#region Fields
			private CultureInfo locale;
			#endregion

			#region Properties
			public CollationDecomposition Decomposition {
				get { return CollationDecomposition.Canonical; }
			}

			public CollationStrength Strength {
				get { return CollationStrength.Identical; }
			}
			#endregion

			#region Public Methods
			public int Compare(string s1, string s2) {
				return locale.CompareInfo.Compare(s1, s2);
			}

			public bool Equals(string s1, string s2) {
				return String.Compare(s1, s2, false, locale) == 0;
			}

			public CollationKey GetCollationKey(string source) {
				throw new NotSupportedException();
			}

			public int Compare(object x, object y) {
				return Compare(x.ToString(), y.ToString());
			}

			public object Clone() {
				return new SystemCollator((CultureInfo)locale.Clone());
			}
			#endregion
		}
		#endregion
	}
}