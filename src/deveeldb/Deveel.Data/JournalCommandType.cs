// 
//  Copyright 2011  Deveel
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
	/// The kind of command registered in a transaction journal
	/// </summary>
	public enum JournalCommandType : byte {
		/// <summary>
		/// Creation of a table within a transaction.
		/// </summary>
		CreateTable = 1,

		/// <summary>
		/// Table dropping during a transaction.
		/// </summary>
		DropTable = 2,

		/// <summary>
		/// Row addition to a table.
		/// </summary>
		AddRow = 3,

		/// <summary>
		/// Row removal from a table.
		/// </summary>
		RemoveRow = 4,

		/// <summary>
		/// Row addition to a table during an update statement.
		/// </summary>
		UpdateAddRow = 5,

		/// <summary>
		/// Row removal from a table during an update statement.
		/// </summary>
		UpdateRemoveRow = 6,

		/// <summary>
		/// A constraint alter
		/// </summary>
		ConstraintAlter = 7
	}
}