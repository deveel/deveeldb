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
	public sealed class UniqueKeyViolationException : ConstraintViolationException {

		internal UniqueKeyViolationException(ObjectName tableName, string constraintName, string[] columnNames,
			ConstraintDeferrability deferrability)
			: base(SystemErrorCodes.UniqueKeyViolation, FormatMessage(tableName, constraintName, columnNames, deferrability)) {
			TableName = tableName;
			ConstraintName = constraintName;
			ColumnNames = columnNames;
			Deferrability = deferrability;
		}

		private static string FormatMessage(ObjectName tableName, string constraintName, string[] columnNames, ConstraintDeferrability deferrability) {
			return String.Format("{0} UNIQUE KEY violation for constraint '{1}({2})' on table '{3}'.",
	deferrability.AsDebugString(), constraintName, String.Join(", ", columnNames), tableName);

		}

		public ObjectName TableName { get; private set; }

		public string ConstraintName { get; private set; }

		public string[] ColumnNames { get; private set; }

		public ConstraintDeferrability Deferrability { get; private set; }
	}
}
