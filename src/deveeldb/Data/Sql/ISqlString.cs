// 
//  Copyright 2010-2018 Deveel
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
//

using System;
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Represents the abstraction of a SQL string
	/// </summary>
	/// <remarks>
	/// <para>
	/// The handling of strings in a SQL system can be different
	/// depending on the kind and length of data to be handled,
	/// and abstraction of access to strings is a good practice
	/// to be able to operate on them independently from their 
	/// representation.
	/// </para>
	/// </remarks>
	public interface ISqlString : ISqlValue, IComparable<ISqlString>, IEnumerable<char> {
		/// <summary>
		/// Gets the length of the string.
		/// </summary>
		long Length { get; }

		/// <summary>
		/// Gets a character at the given index within the string
		/// </summary>
		/// <param name="offset">The zero-based offset at which to get
		/// the character from.</param>
		/// <returns>
		/// Returns a <see cref="char"/> at the given offset within the 
		/// string.
		/// </returns>
		/// <exception cref="ArgumentOutOfRangeException">If the given
		/// <paramref name="offset"/> is less than 0 or past than the
		/// end of the string.</exception>
		char this[long offset] { get; }

		/// <summary>
		/// Gets an object used to read the string in a sequential order
		/// </summary>
		/// <returns>
		/// Returns an instance <see cref="TextReader"/> that is used
		/// to read the content of the string in a sequential order.
		/// </returns>
		TextReader GetInput();
	}
}