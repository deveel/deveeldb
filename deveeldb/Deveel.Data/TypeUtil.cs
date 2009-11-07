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

using Deveel.Math;

namespace Deveel.Data {
	/// <summary>
	/// Utility for converting to and from <see cref="DbType"/> objects.
	/// </summary>
	public class TypeUtil {

		/// <summary>
		/// Converts from a <see cref="Type"/> object to a type as specified in <see cref="DbType"/>.
		/// </summary>
		/// <param name="type"></param>
		/// <returns></returns>
		public static DbType ToDbType(Type type) {
			if (type == typeof (String))
				return DbType.String;
			if (type == typeof (BigDecimal))
				return DbType.Numeric;
			if (type == typeof (DateTime))
				return DbType.Time;
			if (type == typeof (Boolean))
				return DbType.Boolean;
			if (type == typeof (ByteLongObject))
				return DbType.Blob;
			return DbType.Object;
		}

		/// <summary>
		/// Converts from a db type to a <see cref="Type"/> object.
		/// </summary>
		/// <param name="type"></param>
		/// <returns></returns>
		public static Type ToType(DbType type) {
			if (type == DbType.String)
				return typeof(String);
			if (type == DbType.Numeric)
				return typeof(BigDecimal);
			if (type == DbType.Time)
				return typeof(DateTime);
			if (type == DbType.Boolean)
				return typeof(Boolean);
			if (type == DbType.Blob)
				return typeof(ByteLongObject);
			if (type == DbType.Object)
				return typeof(Object);
			throw new ApplicationException("Unknown type.");
		}
	}
}