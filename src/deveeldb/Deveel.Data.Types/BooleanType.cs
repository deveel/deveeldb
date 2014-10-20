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

using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Types {
	[Serializable]
	public sealed class BooleanType : DataType {
		public BooleanType(SqlTypeCode sqlType) 
			: base("BOOLEAN", sqlType) {
			AssertIsBoolean(sqlType);
		}

		private static void AssertIsBoolean(SqlTypeCode sqlType) {
			if (sqlType != SqlTypeCode.Bit &&
				sqlType != SqlTypeCode.Boolean)
				throw new ArgumentException(String.Format("The SQL type {0} is not BOOLEAN.", sqlType));
		}

		public override int Compare(ISqlObject x, ISqlObject y) {
			if (!(x is SqlBoolean))
				throw new ArgumentException();

			var a = (SqlBoolean) x;
			SqlBoolean b;

			if (y is SqlNumber) {
				b = ((SqlNumber) y) == SqlNumber.One ? SqlBoolean.True : ((SqlNumber) y) == SqlNumber.Zero ? SqlBoolean.False : SqlBoolean.Null;
			} else if (y is SqlBoolean) {
				b = (SqlBoolean) y;
			} else {
				throw new ArgumentException();
			}

			return a.CompareTo(b);
		}

		public override bool IsComparable(DataType type) {
			return type is BooleanType || type is NumericType;
		}
	}
}