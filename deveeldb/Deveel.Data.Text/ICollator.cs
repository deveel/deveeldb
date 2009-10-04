//  
//  ICollator.cs
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
using System.Collections;

namespace Deveel.Data.Text {
	/// <summary>
	/// Performs locale-sensitive string comparison.
	/// </summary>
	public interface ICollator : IComparer, ICloneable {
		/// <summary>
		/// Gets the decomposition mode of the collator that determines
		/// how Unicode composed characters are handled.
		/// </summary>
		CollationDecomposition Decomposition { get; }

		/// <summary>
		/// Gets the strength property of the collator that determines the 
		/// minimum level of difference considered significant.
		/// </summary>
		CollationStrength Strength { get; }

		/// <summary>
		/// Compares the source text string to the target text string according 
		/// to the collator's rules, strength and decomposition mode.
		/// </summary>
		/// <param name="s1">The source string of the comparison.</param>
		/// <param name="s2">The target string of the comparison.</param>
		/// <returns>
		/// Returns a value less than zero if source is less than target, 
		/// zero if source and target are equal, greater than zero if source 
		/// is greater than target.
		/// </returns>
		int Compare(string s1, string s2);

		bool Equals(string s1, string s2);

		/// <summary>
		/// Transforms the string into a <see cref="CollationKey"/> suitable 
		/// for efficient repeated comparison, that depends on the collator's
		/// rules, strength and decomposition mode.
		/// </summary>
		/// <param name="source">The source string to be transformed.</param>
		/// <returns>
		/// Returns the <see cref="CollationKey"/> for the given string based on the 
		/// collator's collation rules, or <b>null</b> if the given <paramref name="source"/>
		/// is <b>null</b>.
		/// </returns>
		CollationKey GetCollationKey(string source);
	}
}