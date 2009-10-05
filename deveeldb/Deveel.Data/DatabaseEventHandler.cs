//  
//  DatabaseEventHandler.cs
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
//

using System;

namespace Deveel.Data {
	/// <summary>
	/// A default implementation of <see cref="DatabaseEventHandler"/> which
	/// takes a <see cref="EventHandler"/> as argument to create a generic 
	/// event to dispatch within a database context.
	/// </summary>
	/// <remarks>
	/// In this implementation the <see cref="Execute"/> method calls
	/// the <see cref="EventHandler.Invoke"/> method with a <b>null</b>
	/// sender and an <see cref="EventArgs.Empty">empty</see> argument.
	/// </remarks>
	internal class DatabaseEventHandler : IDatabaseEvent {
		public DatabaseEventHandler(EventHandler handler) {
			this.handler = handler;
		}

		private readonly EventHandler handler;

		public void Execute() {
			handler(null, EventArgs.Empty);
		}
	}
}