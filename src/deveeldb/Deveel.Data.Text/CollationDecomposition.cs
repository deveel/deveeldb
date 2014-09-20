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