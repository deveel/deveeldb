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

namespace Deveel.Data.Sql {
	public static class SqlValueUtil {
		public static ISqlValue FromObject(object value) {
			if (value == null)
				return SqlNull.Value;

			if (value is bool)
				return (SqlBoolean)(bool)value;

			if (value is byte)
				return (SqlNumber)(byte)value;
			if (value is int)
				return (SqlNumber)(int)value;
			if (value is short)
				return (SqlNumber)(short)value;
			if (value is long)
				return (SqlNumber)(long)value;
			if (value is float)
				return (SqlNumber)(float)value;
			if (value is double)
				return (SqlNumber)(double)value;

			if (value is string)
				return new SqlString((string)value);

			if (value is ISqlValue)
				return (ISqlValue) value;

			throw new NotSupportedException();
		}
	}
}