//  
//  RefinedSoundex.cs
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