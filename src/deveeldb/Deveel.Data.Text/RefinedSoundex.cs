// 
//  Copyright 2010  Deveel
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
using System.Text;

namespace Deveel.Data.Text {
	public sealed class RefinedSoundex {
		#region ctor
		public RefinedSoundex(char[] mappings) {
			this.mappings = mappings;
		}
		#endregion

		#region Fields
		private char[] mappings;

		private static readonly string UsEnglishCodes = "01360240043788015936020505";

		public static RefinedSoundex UsEnglish = new RefinedSoundex(UsEnglishCodes.ToCharArray());
		#endregion

		#region Private Methods
		private char GetCode(char c) {
			if (!Char.IsLetter(c))
				return Char.MinValue;
			return mappings[Char.ToUpper(c) - 'A'];
		}
		#endregion

		#region Public Methods
		public string Compute(string s) {
			if (s == null)
				return null;

			s = Soundex.Clean(s);
			if (s.Length == 0)
				return s;

			StringBuilder sb = new StringBuilder();
			sb.Append(s[0]);

			char last = '*';
			char current;

			int i = 0;
			while(i < s.Length) {
				current = GetCode(s[i]);

				if (current != last) {
					sb.Append(current);
					current = last;
				}

				i++;
			}

			return sb.ToString();
		}
		#endregion
	}
}