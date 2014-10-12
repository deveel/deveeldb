// 
//  Copyright 2014  Deveel
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

using Deveel.Data.Sql;

namespace Deveel.Data.Types {
	[Serializable]
	public sealed class BooleanType : DataType {
		public BooleanType(SqlType sqlType) 
			: base("BOOLEAN", sqlType) {
			AssertIsBoolean(sqlType);
		}

		private static void AssertIsBoolean(SqlType sqlType) {
			if (sqlType != SqlType.Bit &&
				sqlType != SqlType.Boolean)
				throw new ArgumentException(String.Format("The SQL type {0} is not BOOLEAN.", sqlType));
		}
	}
}