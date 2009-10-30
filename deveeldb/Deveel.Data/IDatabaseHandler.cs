//  
//  IDatabaseHandler.cs
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

namespace Deveel.Data {
	/// <summary>
	/// An interface implemented by classes which manage databases
	/// within the current system.
	/// </summary>
	public interface IDatabaseHandler {
		/// <summary>
		/// Gets the database instance for the given name.
		/// </summary>
		/// <param name="name">The name of the database to return.</param>
		/// <returns>
		/// Returns an instance of <see cref="Database"/> that is identified
		/// by the <paramref name="name"/> given, or <b>null</b> if none
		/// was specified for the identifier.
		/// </returns>
		/// <exception cref="ArgumentNullException">
		/// Thrown if <paramref name="name"/> is <b>null</b> or an empty string.
		/// </exception>
		Database GetDatabase(string name);
	}
}