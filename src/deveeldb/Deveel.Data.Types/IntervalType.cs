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
	public sealed class IntervalType : DataType {
		public IntervalType(SqlTypeCode sqlType) 
			: base(GetTypeString(sqlType), sqlType) {
			if (sqlType != SqlTypeCode.YearToMonth &&
				sqlType != SqlTypeCode.DayToSecond)
				throw new ArgumentException(String.Format("SQL Type {0} is not a valid INTERVAL.", sqlType.ToString().ToUpperInvariant()));
		}

		private static string GetTypeString(SqlTypeCode sqlType) {
			if (sqlType == SqlTypeCode.DayToSecond)
				return "DAY TO SECOND";
			if (sqlType == SqlTypeCode.YearToMonth)
				return "YEAR TO MONTH";

			return "INTERVAL";
		}

		/// <inheritdoc/>
		public override bool IsComparable(DataType type) {
			if (!(type is IntervalType))
				return false;

			// TODO: better check ...
			return SqlType.Equals(type.SqlType);
		}
	}
}