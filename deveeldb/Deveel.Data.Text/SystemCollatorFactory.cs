//  
//  SystemCollatorFactory.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

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