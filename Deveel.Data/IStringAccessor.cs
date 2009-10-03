// 
//  IStringAccessor.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
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