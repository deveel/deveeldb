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