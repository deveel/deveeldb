// 
//  Copyright 2010-2015 Deveel
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
//


using System;

namespace Deveel.Data.Sql.Triggers {
	/// <summary>
	/// The different types of high layer trigger events.
	/// </summary>
	[Flags]
	public enum TriggerEventType {
		///<summary>
		/// The modification event of an <c>INSERT</c> of values into a table.
		///</summary>
		Insert = 0x001,

		///<summary>
		/// The modification event of a <c>DELETE</c> of values from a table.
		///</summary>
		Delete = 0x002,

		///<summary>
		/// The modification event of <c>UPDATE</c> of field values in a table.
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

		/// <summary>
		/// An event that occurs <c>BEFORE</c> a <c>DELETE</c> from a table.
		/// </summary>
		BeforeDelete = Before | Delete,

		/// <summary>
		/// An event that occurs <c>BEFORE</c> an <c>UPDATE</c> on a table.
		/// </summary>
		BeforeUpdate = Before | Update,

		/// <summary>
		/// An event that occurs <c>AFTER</c> an <c>INSERT</c> on a table.
		/// </summary>
		AfterInsert = After | Insert,

		AfterUpdate = After | Update,

		AfterDelete = After | Delete,

		AllBefore = BeforeInsert | BeforeDelete | BeforeUpdate,
		AllAfter = AfterInsert | AfterDelete | AfterUpdate,
	}
}