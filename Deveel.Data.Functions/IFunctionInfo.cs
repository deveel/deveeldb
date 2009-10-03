// 
//  IFunctionInfo.cs
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

namespace Deveel.Data.Functions {
	/// <summary>
	/// Meta information about a function, used to compile information 
	/// about a particular function.
	/// </summary>
	public interface IFunctionInfo {
		/// <summary>
		/// The name of the function as used by the SQL grammar to reference it.
		/// </summary>
		string Name { get; }

		/// <summary>
		/// The type of function, either Static, Aggregate or StateBased (eg. result
		/// is not dependant entirely from input but from another state for example
		/// RANDOM and UNIQUEKEY functions).
		/// </summary>
		FunctionType Type { get; }

		/// <summary>
		/// The name of the function factory class that this function is handled by.
		/// </summary>
		string FunctionFactoryName { get; }
	}
}