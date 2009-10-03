// 
//  SQLTypes.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
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

using System;

namespace Deveel.Data {
	public enum SQLTypes {
		BIT = -7,

		TINYINT = -6,

		SMALLINT = 5,

		INTEGER = 4,

		BIGINT = -5,

		FLOAT = 6,

		REAL = 7,

		DOUBLE = 8,

		NUMERIC = 2,

		DECIMAL = 3,

		CHAR = 1,

		VARCHAR = 12,

		LONGVARCHAR = -1,

		DATE = 91,

		TIME = 92,

		TIMESTAMP = 93,

		BINARY = -2,

		VARBINARY = -3,

		LONGVARBINARY = -4,

		NULL = 0,

		OTHER = 1111,

		OBJECT = 2000,

		DISTINCT = 2001,

		STRUCT = 2002,

		ARRAY = 2003,

		BLOB = 2004,

		CLOB = 2005,

		REF = 2006,

		BOOLEAN = 16,

		QUERY_PLAN_NODE = -19443,

		UNKNOWN = -9332
	}
}