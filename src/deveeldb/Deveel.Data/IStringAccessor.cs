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
using System.IO;

namespace Deveel.Data {
	/// <summary>
	/// An interface used by the engine to access and process strings.
	/// </summary>
	/// <remarks>
	/// This interface allows us to access the contents of a string that may be
	/// implemented in several different ways.  For example, a string may be 
	/// represented as a <see cref="string"/> object in memeory, or it may be
	/// represented as an ASCII sequence in a store.
	/// </remarks>
	public interface IStringAccessor {
		/// <summary>
		/// Gets the number of characters in the string.
		/// </summary>
		int Length { get; }

		/// <summary>
		/// Returns a <see cref="TextReader"/> that allows the string to be read 
		/// sequentually from start to finish.
		/// </summary>
		/// <returns></returns>
		TextReader GetTextReader();

		/// <summary>
		/// Returns this string as a <see cref="string"/> object.
		/// </summary>
		/// <remarks>
		/// Some care may be necessary with this call because a 
		/// very large string will require a lot space on the heap.
		/// </remarks>
		/// <returns></returns>
		string ToString();
	}
}