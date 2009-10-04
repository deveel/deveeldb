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

namespace Deveel.Data {
	///<summary>
	/// A listener that can listen for high layer trigger events.
	///</summary>
	public interface ITriggerListener {
		///<summary>
		/// Notifies that a trigger event fired.
		///</summary>
		///<param name="database">The <see cref="DatabaseConnection"/> that this 
		/// trigger is registered for.</param>
		///<param name="trigger_name">The name of the trigger fired.</param>
		///<param name="trigger_evt">The trigger event that was fired.</param>
		void FireTrigger(DatabaseConnection database, String trigger_name, TriggerEvent trigger_evt);
	}
}