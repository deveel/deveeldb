// 
//  Copyright 2010-2015 Deveel
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
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class ConstraintInfo {
		public ConstraintInfo(ConstraintType constraintType, ObjectName tableName, string[] columnNames) 
			: this(null, constraintType, tableName, columnNames) {
		}

		public ConstraintInfo(ObjectName constraintName, ConstraintType constraintType, ObjectName tableName, string[] columnNames) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");
			if (columnNames == null)
				throw new ArgumentNullException("columnNames");

			if (columnNames.Length == 0)
				throw new ArgumentException("The provided column names for the constraint is empty.", "columnNames");

			ConstraintName = constraintName;
			ColumnNames = columnNames;
			TableName = tableName;
			ConstraintType = constraintType;
		}

		public ConstraintType ConstraintType { get; private set; }

		public ObjectName TableName { get; private set; }

		public ObjectName ConstraintName { get; set; }

		public string[] ColumnNames { get; private set; }

		public ObjectName ForeignTable { get; set; }

		public string[] ForeignColumnNames { get; set; }

		public ForeignKeyAction OnDelete { get; set; }

		public ForeignKeyAction OnUpdate { get; set; }

		public static ConstraintInfo Unique(string constraintName, ObjectName tableName, string[] columnNames) {
			return Unique(ObjectName.Parse(constraintName), tableName, columnNames);
		}

		public static ConstraintInfo Unique(ObjectName tableName, string[] columnNames) {
			return Unique((ObjectName)null, tableName, columnNames);
		}

		public static ConstraintInfo Unique(ObjectName constraintName, ObjectName tableName, string[] columnNames) {
			return new ConstraintInfo(constraintName, ConstraintType.Unique, tableName, columnNames);
		}
	}
}