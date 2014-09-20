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

namespace Deveel.Data.Transactions {
	/// <summary>
	/// A command registered in a table or a transaction journal
	/// </summary>
	public struct JournalCommand {
		private readonly JournalCommandType commandType;
		private readonly int tableId;
		private readonly int rowIndex;

		/// <summary>
		/// Constructs a journal command.
		/// </summary>
		/// <param name="commandType"></param>
		/// <param name="tableId"></param>
		public JournalCommand(JournalCommandType commandType, int tableId) 
			: this(commandType, tableId, -1) {
		}

		/// <summary>
		/// Constructs a journal command.
		/// </summary>
		/// <param name="commandType"></param>
		/// <param name="tableId"></param>
		/// <param name="rowIndex"></param>
		public JournalCommand(JournalCommandType commandType, int tableId, int rowIndex) 
			: this() {
			this.commandType = commandType;
			this.tableId = tableId;
			this.rowIndex = rowIndex;
		}

		/// <summary>
		/// Gets the index of a row argument of the command.
		/// </summary>
		public int RowIndex {
			get { return rowIndex; }
		}

		/// <summary>
		/// Gets the table id argument of the command.
		/// </summary>
		public int TableId {
			get { return tableId; }
		}

		/// <summary>
		/// Gets the kind of command.
		/// </summary>
		public JournalCommandType CommandType {
			get { return commandType; }
		}
	}
}