﻿// 
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
	public sealed class DropTableViolationException : ConstraintViolationException {
		internal DropTableViolationException(ObjectName tableName, string constraintName, ObjectName linkedTableName)
			: base(SystemErrorCodes.TableDropViolation, FormatMessage(tableName, constraintName, linkedTableName)) {
			TableName = tableName;
			ConstraintName = constraintName;
			LinkedTableName = linkedTableName;
		}

		public ObjectName TableName { get; private set; }

		public string ConstraintName { get; private set; }

		public ObjectName LinkedTableName { get; private set; }

		private static string FormatMessage(ObjectName tableName, string constraintName, ObjectName linkedTableName) {
			return String.Format("Attempt to DROP the table '{0}' that is linked by the constraint '{1}' to table '{2}'.", tableName, constraintName, linkedTableName);
		}
	}
}
