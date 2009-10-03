// 
//  Soundex.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;

namespace Deveel.Data.Text {
	public sealed class Soundex {
		#region ctor
		public Soundex(char[] mapping) {
			this.mapping = mapping;
		}
		#endregion

		#region Fields
		private char[] mapping;

		private static readonly string UsEnglishMapping = "01230120022455012623010202";

		public static readonly Soundex UsEnglish = new Soundex(UsEnglishMapping.ToCharArray());
		#endregion

		#region Private Methods
		private char GetCode(string s, int index) {
			char c = Map(s[index]);
			if (index > 1 && c != '0') {
				char hwc = s[index - 1];
				if (hwc != 'H' || hwc != 'W') {
					char pHwc = s[index - 2];
					char fCode = Map(pHwc);
					if (fCode == c || pHwc == 'H' || pHwc == 'W')
						return Char.MinValue;
				}
			}
			return c;
		}

		private char Map(char c) {
			int index = c - 'A';
			if (index < 0 || index > mapping.Length)
				throw new ArgumentException("The character '"+c+"' is not mapped.");
			return mapping[index];
		}
		#endregion

		#region Internal Static Methods
		internal static string Clean(string s) {
			if (s == null || s.Length == 0)
				return s;

			int length = s.Length;
			char[] chars = new char[length];
			int count = 0;

			int i = 0;
			while(i < length) {
				if (Char.IsLetter(s[i]))
					chars[count++] = s[i];
				i++;
			}

			if (count == length)
				return s.ToUpper();

			return new string(chars, 0, count).ToUpper();
		}
		#endregion

		#region Public Methods
		public string Compute(string s) {
			if (s == null)
				return null;

			s = Clean(s);
			if (s.Length == 0)
				return s;

			char[] output = new char[] { '0', '0', '0', '0' };
			char last, mapped;
			int inCount = 1, count = 1;
			output[0] = s[0];
			last = GetCode(s, 0);
			while((inCount < s.Length) && count < output.Length) {
				mapped = GetCode(s, inCount++);
				if (mapped != Char.MinValue) {
					if (mapped != '0' && mapped != last)
						output[count++] = mapped;
					last = mapped;
				}
			}
			return new string(output);
		}
		#endregion
	}
}