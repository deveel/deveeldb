// 
//  Copyright 2010-2016 Deveel
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

namespace Deveel.Data.Sql.Tables {
	public sealed class NotNullColumnViolationException : ConstraintViolationException {
		internal NotNullColumnViolationException(ObjectName tableName, string columnName)
			: base(SystemErrorCodes.NotNullColumnViolation, FormatMessage(tableName, columnName)) {
			TableName = tableName;
			ColumnName = columnName;
		}

		public ObjectName TableName { get; private set; }

		public string ColumnName { get; private set; }

		private static string FormatMessage(ObjectName tableName, string columnName) {
			return String.Format("Attempt to set NULL to the column '{0}' of table '{1}'.", columnName, tableName);
		}
	}
}
