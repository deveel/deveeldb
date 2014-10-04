// 
//  Copyright 2010-2014 Deveel
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

using System;

namespace Deveel.Data.Routines {
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

		/// <summary>
		/// An event that occurs <c>AFTER</c> an <c>INSERT</c> on a table.
		/// </summary>
		BeforeInsert = Before | Insert,

		BeforeDelete = Before | Delete,

		BefroeUpdate = Before | Update,

		AfterInsert = After | Insert,

		AfterUpdate = After | Update,

		AfterDelete = After | Delete,

		AllBefore = BeforeInsert | BeforeDelete | BefroeUpdate,
		AllAfter = AfterInsert | AfterDelete | AfterUpdate,
	}
}