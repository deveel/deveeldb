//  
//  ITriggerListener.cs
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

namespace Deveel.Data.Client {
	/// <summary>
	/// A listener that is notified when the trigger being listened to is fired.
	/// </summary>
	public interface ITriggerListener {
		/// <summary>
		/// Notifies this listener that the trigger with the name has been fired.
		/// </summary>
		/// <param name="e">The argument object that describes the information
		/// about a fired trigger.</param>
		/// <remarks>
		/// Trigger's are specified via the SQL syntax and a trigger listener can 
		/// be registered via <see cref="DeveelDbConnection"/>.
		/// </remarks>
		/// <seealso cref="TriggerEventArgs"/>
		void OnTriggerFired(TriggerEventArgs e);
	}
}