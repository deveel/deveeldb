// 
//  Copyright 2010-2014 Deveel
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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Types;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class ColumnInfo {
		public ColumnInfo(string columnName, DataType columnType) {
			if (String.IsNullOrEmpty(columnName))
				throw new ArgumentNullException("columnName");

			ColumnType = columnType;
			ColumnName = columnName;
		}

		public TableInfo TableInfo { get; internal set; }

		public string ColumnName { get; private set; }

		public ObjectName FullColumnName {
			get { return TableInfo == null ? new ObjectName(ColumnName) : new ObjectName(TableInfo.TableName, ColumnName); }
		}

		public DataType ColumnType { get; private set; }

		public int Offset {
			get { return TableInfo == null ? -1 : TableInfo.IndexOfColumn(ColumnName); }
		}

		public bool IsIndexable {
			get { return ColumnType.IsIndexable; }
		}

		public bool IsNotNull { get; set; }

		public SqlExpression DefaultExpression { get; set; }

		public bool HasDefaultExpression {
			get { return DefaultExpression != null; }
		}
	}
}