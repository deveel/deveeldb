// 
//  DeveelDbType.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

namespace Deveel.Data.Client {
	public enum DeveelDbType : byte {
		Unknown = 0,
		Null = 1,
		String = 18,
		Int4 = 24,
		Int8 = 8,
		Number = 7,
		Boolean = 12,
		Time = 9,
		Interval = 10,
		Binary = 15,
		LOB = 16,
		UDT = 32
	}
}