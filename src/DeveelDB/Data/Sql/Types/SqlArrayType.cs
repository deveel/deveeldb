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

namespace Deveel.Data.Sql.Types {
	public sealed class SqlArrayType : SqlType {
		public SqlArrayType(int length)
			: base(SqlTypeCode.Array) {
			if (length < 0)
				throw new ArgumentException("Invalid array length");

			Length = length;
		}

		public int Length { get; }

		public override bool IsInstanceOf(ISqlValue value) {
			return value is SqlArray && ((SqlArray) value).Length == Length;
		}

		public override bool Equals(SqlType other) {
			if (!(other is SqlArrayType))
				return false;

			var otherType = (SqlArrayType) other;
			return Length == otherType.Length;
		}
	}
}