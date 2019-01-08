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

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql {
	public static class SqlTypeUtil {
		public static SqlType FromValue(object value) {
			if (value is SqlNumber) {
				var number = (SqlNumber) value;
				if (number.Scale == 0) {
					if (number.Precision <= 3)
						return PrimitiveTypes.TinyInt();
					if (number.Precision <= 5)
						return PrimitiveTypes.SmallInt();
					if (number.Precision <= 10)
						return PrimitiveTypes.Integer();
					if (number.Precision <= 19)
						return PrimitiveTypes.BigInt();
				} else {
					if (number.Precision <= 8)
						return PrimitiveTypes.Float();
					if (number.Precision <= 12)
						return PrimitiveTypes.Double();

					return PrimitiveTypes.Numeric(number.Precision, number.Scale);
				}
			}

			if (value is SqlBoolean)
				return PrimitiveTypes.Boolean();

			if (value is bool)
				return PrimitiveTypes.Boolean();

			if (value is double)
				return PrimitiveTypes.Double();
			if (value is float)
				return PrimitiveTypes.Float();
			if (value is int)
				return PrimitiveTypes.Integer();
			if (value is long)
				return PrimitiveTypes.BigInt();
			if (value is byte)
				return PrimitiveTypes.TinyInt();
			if (value is short)
				return PrimitiveTypes.SmallInt();

			if (value is string)
				return PrimitiveTypes.String();

			throw new NotSupportedException();
		}
	}
}