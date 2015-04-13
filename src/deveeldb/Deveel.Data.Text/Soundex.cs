// 
//  Copyright 2010-2015 Deveel
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
using System.Text;

namespace Deveel.Data.Text {
	public abstract class Soundex {
		public static Soundex Default = new DefaultSoundex();

		public virtual int Difference(string s1, string s2) {
			throw new NotSupportedException();
		}

		public abstract string Compute(string s);

		protected virtual string EncodeChar(char c) {
			switch (Char.ToUpperInvariant(c)) {
				case 'B':
				case 'F':
				case 'P':
				case 'V':
					return "1";
				case 'C':
				case 'G':
				case 'J':
				case 'K':
				case 'Q':
				case 'S':
				case 'X':
				case 'Z':
					return "2";
				case 'D':
				case 'T':
					return "3";
				case 'L':
					return "4";
				case 'M':
				case 'N':
					return "5";
				case 'R':
					return "6";
				default:
					return string.Empty;
			}
		}

		#region DefaultSoundex

		private class DefaultSoundex : Soundex {
			public override string Compute(string s) {
				if (String.IsNullOrEmpty(s)) 
					return String.Empty;

				int startIndex;
				for (startIndex = 0; startIndex < s.Length && !char.IsLetter(s[startIndex]); startIndex++) {
				}

				if (startIndex >= s.Length) 
					return String.Empty;

				var output = new StringBuilder();

				output.Append(Char.ToUpperInvariant(s[startIndex]));

				// Stop at a maximum of 4 characters.
				for (int i = startIndex + 1; i < s.Length && output.Length < 4; i++) {
					string c = EncodeChar(s[i]);

					// Ignore duplicated chars.
					if (c != EncodeChar(s[i - 1])) {
						output.Append(c);
					}
				}

				// Pad with zeros.
				output.Append(new String('0', 4 - output.Length));

				return output.ToString();
			}
		}

		#endregion
	}
}