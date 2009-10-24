//  
//  TriggerEventType.cs
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
	/// The different types of high layer trigger events.
	/// </summary>
	[Flags]
	public enum TriggerEventType {
		///<summary>
		///</summary>
		Insert = 0x001,
		///<summary>
		///</summary>
		Delete = 0x002,
		///<summary>
		///</summary>
		Update = 0x004,

		/// <summary>
		/// An event that occurs <c>BEFORE</c> the modification of a 
		/// table contents.
		/// </summary>
		Before = 0x010,

		/// <summary>
		/// An event that occurs <c>AFTER</c> the modification of a 
		/// table contents.
		/// </summary>
		After = 0x020,

		BeforeInsert = Before | Insert,

		BeforeDelete = Before | Delete,

		BefroeUpdate = Before | Update,

		AfterInsert = After | Insert,

		AfterUpdate = After | Update,

		AfterDelete = After | Delete
	}
}