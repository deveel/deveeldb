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