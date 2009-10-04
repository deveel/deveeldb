//  
//  JournalCommand.cs
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
	internal class JournalCommand {
		// (params: table_id, row_index)
		internal const byte TABLE_ADD = 1;         // Add a row to a table.
		// (params: table_id, row_index)
		internal const byte TABLE_REMOVE = 2;         // Remove a row from a table.
		internal const byte TABLE_UPDATE_ADD = 5;  // Add a row from an update.
		internal const byte TABLE_UPDATE_REMOVE = 6;  // Remove a row from an update.
	}
}