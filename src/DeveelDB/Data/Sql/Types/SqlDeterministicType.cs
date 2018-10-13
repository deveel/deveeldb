// 
//  Copyright 2010-2017 Deveel
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
	public sealed class SqlDeterministicType : SqlType {
		public SqlDeterministicType()
			: base(SqlTypeCode.Unknown) {
		}

		public override bool IsComparable(SqlType type) {
			return false;
		}

		public override bool CanCastTo(ISqlValue value, SqlType destType) {
			return false;
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.Append("DETERMINISTIC");
		}
	}
}