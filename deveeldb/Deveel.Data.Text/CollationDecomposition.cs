//  
//  CollationDecomposition.cs
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

namespace Deveel.Data.Text {
	public enum CollationDecomposition {
		/// <summary>
		/// Both canonical variants and compatibility variants in Unicode 
		/// 2.0 will be decomposed prior to performing comparisons. 
		/// </summary>
		/// <remarks>
		/// This is the slowest mode, but is required to get the correct 
		/// sorting for certain languages with certain special formats.
		/// </remarks>
		Full      = 15,

		/// <summary>
		/// Accented characters won't be decomposed when performing 
		/// comparisons.
		/// </summary>
		/// <remarks>
		/// This will yield the fastest results, but will only work correctly 
		/// in call cases for languages which do not use accents such as English.
		/// </remarks>
		None      = 16,

		/// <summary>
		/// Only characters which are canonical variants in Unicode 
		/// 2.0 will be decomposed prior to performing comparisons.
		/// </summary>
		/// <remarks>
		/// This will cause accented languages to be sorted correctly. This 
		/// is the default decomposition value.
		/// </remarks>
		Canonical = 17
	}
}