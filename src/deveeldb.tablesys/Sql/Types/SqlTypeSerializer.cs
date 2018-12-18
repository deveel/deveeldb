// 
//  Copyright 2010-2018 Deveel
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
using System.IO;

namespace Deveel.Data.Sql.Types {
	public static class SqlTypeSerializer {
		public static void Serialize(SqlType type, BinaryWriter writer) {
			writer.Write((byte) type.TypeCode);

			if (type.IsPrimitive) {
				if (type is SqlNumericType) {
					var numericType = (SqlNumericType) type;
					writer.Write(numericType.Precision);
					writer.Write(numericType.Scale);
				} else if (type is SqlCharacterType) {
					var stringType = (SqlCharacterType) type;
					writer.Write(stringType.MaxSize);

					if (stringType.Locale != null) {
						writer.Write((byte) 1);
						writer.Write(stringType.Locale.Name);
					} else {
						writer.Write((byte) 0);
					}
				} else if (type is SqlBinaryType) {
					var binaryType = (SqlBinaryType) type;

					writer.Write(binaryType.MaxSize);
				} else if (type is SqlBooleanType ||
				           type is SqlYearToMonthType ||
				           type is SqlDateTimeType) {
					// nothing to add to the SQL Type Code
				} else {
					throw new NotSupportedException($"The data type '{type.GetType().FullName}' cannot be serialized.");
				}
			}
			/* TODO:
			else if (type is UserType) {
				var userType = (UserType) type;
				writer.Write((byte) 1); // The code of custom type
				writer.Write(userType.FullName.FullName);
			} else if (type is QueryType) {
				// nothing to do for the Query Type here
			} */
			else if (type is SqlArrayType) {
				var arrayType = (SqlArrayType) type;
				writer.Write(arrayType.Length);
			} else {
				throw new NotSupportedException();
			}
		}

		public static SqlType Deserialize(BinaryReader reader, ISqlTypeResolver resolver) {
			throw new NotImplementedException();
		}
	}
}