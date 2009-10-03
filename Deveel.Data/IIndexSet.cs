// 
//  IIndexSet.cs
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

using Deveel.Data.Collections;

namespace Deveel.Data {
	/// <summary>
	/// A set of list of indexes.
	/// </summary>
	/// <remarks>
	/// This will often expose an isolated snapshot of a set of indices 
	/// for a table.
	/// </remarks>
	public interface IIndexSet : IDisposable {
		/// <summary>
		/// Gets a mutable object that implements <see cref="IIntegerList"/>
		/// for the given index number in this set of indices.
		/// </summary>
		/// <param name="index"></param>
		/// <returns></returns>
		IIntegerList GetIndex(int index);
	}
}