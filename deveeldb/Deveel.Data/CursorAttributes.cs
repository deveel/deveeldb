// 
//  Copyright 2010  Deveel
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

namespace Deveel.Data {
	/// <summary>
	/// The flags defining the properties of a <see cref="Cursor"/>.
	/// </summary>
	[Flags]
	public enum CursorAttributes {
		/// <summary>
		/// Marks a cursor as read-only: this is the exact opposite
		/// of the option <see cref="Update"/>.
		/// </summary>
		ReadOnly = 0x01,

		/// <summary>
		/// A cursor marked with this flag can update a given set of
		/// columns of a table or environment variables.
		/// </summary>
		Update = 0x02,

		/// <summary>
		/// The cursor will ignore every modification made to the tables
		/// referenced by the query command forming it after its declaration.
		/// </summary>
		Insensitive = 0x04,

		/// <summary>
		/// Allows fetching directions other than forward.
		/// </summary>
		Scrollable = 0x05
	}
}