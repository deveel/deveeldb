// 
//  Metaphone.cs
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
using System.Text;

namespace Deveel.Data.Text {
	public sealed class Metaphone {
		#region ctor
		public Metaphone() {
		}
		#endregion

		#region Fields
		private string vowels = "AEIOU";
		private string frontv = "EIY";
		private string varson = "CSPTG";
		private int maxCodeLen = 4;
		#endregion

		#region Private Methods
		private bool IsLastChar(int wdsz, int n) {
			return n + 1 == wdsz;
		}

		private bool RegionMatch(StringBuilder sb, int index, String test) {
			bool matches = false;
			if( index >= 0 &&
				(index + test.Length - 1) < sb.Length) {
				string substring = sb.ToString(index, test.Length);
				matches = substring.Equals(test);
			}
			return matches;
		}

		private bool IsVowel(StringBuilder sb, int index) {
			return (this.vowels.IndexOf(sb[index]) >= 0);
		}

		private bool IsPreviousChar(StringBuilder sb, int index, char c) {
			bool matches = false;
			if( index > 0 && index < sb.Length) {
				matches = sb[index - 1] == c;
			}
			return matches;
		}

		private bool IsNextChar(StringBuilder sb, int index, char c) {
			bool matches = false;
			if (index >= 0 && index < sb.Length - 1) {
				matches = sb[index + 1] == c;
			}
			return matches;
		}
		#endregion

		#region Public Methods
		public string Compute(string s) {
			bool hard = false ;
			if (s == null || s.Length == 0)
				return "";

			// single character is itself
			if (s.Length == 1)
				return s.ToUpper();
       
			char[] inwd = s.ToUpper().ToCharArray() ;
       
			StringBuilder local = new StringBuilder(40); // manipulate
			StringBuilder code = new StringBuilder(10) ; //   output
			// handle initial 2 characters exceptions
			switch(inwd[0]) {
				case 'K' : 
				case 'G' : 
				case 'P' : /* looking for KN, etc*/
					if (inwd[1] == 'N') {
						local.Append(inwd, 1, inwd.Length - 1);
					} else {
						local.Append(inwd);
					}
					break;
				case 'A': /* looking for AE */
					if (inwd[1] == 'E') {
						local.Append(inwd, 1, inwd.Length - 1);
					} else {
						local.Append(inwd);
					}
					break;
				case 'W' : /* looking for WR or WH */
					if (inwd[1] == 'R') {   // WR -> R
						local.Append(inwd, 1, inwd.Length - 1); 
						break ;
					}
					if (inwd[1] == 'H') {
						local.Append(inwd, 1, inwd.Length - 1);
						local[0] = 'W'; // WH -> W
					} else {
						local.Append(inwd);
					}
					break;
				case 'X' : /* initial X becomes S */
					inwd[0] = 'S';
					local.Append(inwd);
					break ;
				default :
					local.Append(inwd);
					break;
			} // now local has working string with initials fixed

			int wdsz = local.Length;
			int n = 0 ;

			while ((code.Length < this.maxCodeLen) && 
				(n < wdsz) ) { // max code size of 4 works well
				char symb = local[n];
				// remove duplicate letters except C
				if ((symb != 'C') && (IsPreviousChar( local, n, symb )) ) {
					n++ ;
				} else { // not dup
					switch(symb) {
						case 'A' : case 'E' : case 'I' : case 'O' : case 'U' :
							if (n == 0) { 
								code.Append(symb);
							}
							break ; // only use vowel if leading char
						case 'B' :
							if (IsPreviousChar(local, n, 'M') && 
								IsLastChar(wdsz, n) ) { // B is silent if word ends in MB
								break;
							}
							code.Append(symb);
							break;
						case 'C' : // lots of C special cases
							/* discard if SCI, SCE or SCY */
							if (IsPreviousChar(local, n, 'S') && 
								!IsLastChar(wdsz, n) && 
								(this.frontv.IndexOf(local[n + 1]) >= 0)) {
								break;
							}
							if (RegionMatch(local, n, "CIA")) { // "CIA" -> X
								code.Append('X'); 
								break;
							}
							if (!IsLastChar(wdsz, n) && 
								(this.frontv.IndexOf(local[n + 1]) >= 0)) {
								code.Append('S');
								break; // CI,CE,CY -> S
							}
							if (IsPreviousChar(local, n, 'S') &&
								IsNextChar(local, n, 'H') ) { // SCH-&gtsk
								code.Append('K') ; 
								break ;
							}
							if (IsNextChar(local, n, 'H')) { // detect CH
								if ((n == 0) && 
									(wdsz >= 3) && 
									IsVowel(local,2) ) { // CH consonant -> K consonant
									code.Append('K');
								} else { 
									code.Append('X'); // CHvowel -> X
								}
							} else { 
								code.Append('K');
							}
							break ;
						case 'D' :
							if (!IsLastChar(wdsz, n + 1) && 
								IsNextChar(local, n, 'G') && 
								(this.frontv.IndexOf(local[n + 2]) >= 0)) { // DGE DGI DGY -> J 
								code.Append('J'); n += 2 ;
							} else { 
								code.Append('T');
							}
							break ;
						case 'G' : // GH silent at end or before consonant
							if (IsLastChar(wdsz, n + 1) && 
								IsNextChar(local, n, 'H')) {
								break;
							}
							if (!IsLastChar(wdsz, n + 1) &&  
								IsNextChar(local,n,'H') && 
								!IsVowel(local,n+2)) {
								break;
							}
							if ((n > 0) && 
								(RegionMatch(local, n, "GN") ||
								RegionMatch(local, n, "GNED") ) ) {
								break; // silent G
							}
							if (IsPreviousChar(local, n, 'G')) {
								hard = true ;
							} else {
								hard = false ;
							}
							if (!IsLastChar(wdsz, n) && 
								(this.frontv.IndexOf(local[n + 1]) >= 0) && 
								(!hard)) {
								code.Append('J');
							} else {
								code.Append('K');
							}
							break ;
						case 'H':
							if (IsLastChar(wdsz, n)) {
								break ; // terminal H
							}
							if ((n > 0) && 
								(this.varson.IndexOf(local[n - 1]) >= 0)) {
								break;
							}
							if (IsVowel(local,n+1)) {
								code.Append('H'); // Hvowel
							}
							break;
						case 'F': 
						case 'J' : 
						case 'L' :
						case 'M': 
						case 'N' : 
						case 'R' :
							code.Append(symb); 
							break;
						case 'K' :
							if (n > 0) { // not initial
								if (!IsPreviousChar(local, n, 'C')) {
									code.Append(symb);
								}
							} else {
								code.Append(symb); // initial K
							}
							break ;
						case 'P' :
							if (IsNextChar(local,n,'H')) {
								// PH -> F
								code.Append('F');
							} else {
								code.Append(symb);
							}
							break ;
						case 'Q' :
							code.Append('K');
							break;
						case 'S' :
							if (RegionMatch(local,n,"SH") || 
								RegionMatch(local,n,"SIO") || 
								RegionMatch(local,n,"SIA")) {
								code.Append('X');
							} else {
								code.Append('S');
							}
							break;
						case 'T' :
							if (RegionMatch(local,n,"TIA") || 
								RegionMatch(local,n,"TIO")) {
								code.Append('X'); 
								break;
							}
							if (RegionMatch(local,n,"TCH")) {
								// Silent if in "TCH"
								break;
							}
							// substitute numeral 0 for TH (resembles theta after all)
							if (RegionMatch(local,n,"TH")) {
								code.Append('0');
							} else {
								code.Append('T');
							}
							break ;
						case 'V' :
							code.Append('F');
							break ;
						case 'W' : case 'Y' : // silent if not followed by vowel
							if (!IsLastChar(wdsz,n) && 
								IsVowel(local,n+1)) {
								code.Append(symb);
							}
							break ;
						case 'X' :
							code.Append('K');
							code.Append('S');
							break ;
						case 'Z' :
							code.Append('S');
							break ;
					} // end switch
					n++ ;
				} // end else from symb != 'C'
				if (code.Length > this.maxCodeLen) { 
					code.Length = maxCodeLen; 
				}
			}
			return code.ToString();
		}
		#endregion
	}
}