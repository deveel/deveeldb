// 
//  TypeUtil.cs
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

using Deveel.Math;

namespace Deveel.Data {
	/// <summary>
	/// Utility for converting to and from <see cref="DbTypes"/> objects.
	/// </summary>
	public class TypeUtil {

		/// <summary>
		/// Converts from a <see cref="Type"/> object to a type as specified in <see cref="DbTypes"/>.
		/// </summary>
		/// <param name="type"></param>
		/// <returns></returns>
		public static DbTypes ToDbType(Type type) {
			if (type == typeof (String))
				return DbTypes.DB_STRING;
			if (type == typeof (BigDecimal))
				return DbTypes.DB_NUMERIC;
			if (type == typeof (DateTime))
				return DbTypes.DB_TIME;
			if (type == typeof (Boolean))
				return DbTypes.DB_BOOLEAN;
			if (type == typeof (ByteLongObject))
				return DbTypes.DB_BLOB;
			return DbTypes.DB_OBJECT;
		}

		/// <summary>
		/// Converts from a db type to a <see cref="Type"/> object.
		/// </summary>
		/// <param name="type"></param>
		/// <returns></returns>
		public static Type ToType(DbTypes type) {
			if (type == DbTypes.DB_STRING)
				return typeof(String);
			if (type == DbTypes.DB_NUMERIC)
				return typeof(BigDecimal);
			if (type == DbTypes.DB_TIME)
				return typeof(DateTime);
			if (type == DbTypes.DB_BOOLEAN)
				return typeof(Boolean);
			if (type == DbTypes.DB_BLOB)
				return typeof(ByteLongObject);
			if (type == DbTypes.DB_OBJECT)
				return typeof(Object);
			throw new ApplicationException("Unknown type.");
		}
	}
}