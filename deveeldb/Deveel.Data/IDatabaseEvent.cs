//  
//  IDatabaseEvent.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
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
	/// A generic interface for the implementation of an event to
	/// execute within a <see cref="Database"/> context.
	/// </summary>
	/// <remarks>
	/// Implementations of this interface will be passed to the method
	/// <see cref="Database.CreateEvent"/> to generate an event object.
	/// <para>
	/// The method <see cref="Execute"/> will be called by the system
	/// at the time of firing the event.
	/// </para>
	/// </remarks>
	public interface IDatabaseEvent {
		/// <summary>
		/// Executes the event within the database context.
		/// </summary>
		void Execute();
	}
}
