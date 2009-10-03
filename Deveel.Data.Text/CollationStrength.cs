// 
//  CollationStrength.cs
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

namespace Deveel.Data.Text {
	public enum CollationStrength {
		/// <summary>
		/// Only primary differences between characters will be 
		/// considered signficant.
		/// </summary>
		/// <remarks>
		/// As an example, two completely different English letters 
		/// such as 'a' and 'b' are considered to have a primary difference.
		/// </remarks>
		Primary    = 0,

		/// <summary>
		/// Only secondary or primary differences between characters 
		/// will be considered significant.
		/// </summary>
		/// <remarks>
		/// An example of a secondary difference between characters
		/// are instances of the same letter with different accented forms.
		/// </remarks>
		Secondary  = 1,

		/// <summary>
		/// Tertiary, secondary, and primary differences will be considered 
		/// during sorting.
		/// </summary>
		/// <remarks>
		/// An example of a tertiary difference is capitalization of a given 
		/// letter. This is the default value for the strength setting.
		/// </remarks>
		Tertiary   = 2,

		Quaternary = 3,

		/// <summary>
		/// Any difference at all between character values are considered 
		/// significant.
		/// </summary>
		Identical  = 15,

		None = -1
	}
}