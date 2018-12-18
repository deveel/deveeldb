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

namespace Deveel.Data.Sql.Indexes {
	public sealed class IndexInfo : IDbObjectInfo {
		public IndexInfo(string indexName, ObjectName tableName, string columnName) 
			: this(indexName, tableName, columnName, false) {
		}

		public IndexInfo(string indexName, ObjectName tableName, string columnName, bool unique) 
			: this(indexName, tableName, new [] { columnName }, unique) {
		}

		public IndexInfo(string indexName, ObjectName tableName, string[] columnNames) 
			: this(indexName, tableName, columnNames, false) {
		}

		public IndexInfo(string indexName, ObjectName tableName, string[] columnNames, bool unique) {
			IndexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
			TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
			ColumnNames = columnNames;
			Unique = unique;
		}

		DbObjectType IDbObjectInfo.ObjectType => DbObjectType.Index;

		public string IndexName { get; }

		public ObjectName FullName => new ObjectName(TableName, IndexName);

		public ObjectName TableName { get; }

		public string[] ColumnNames { get; }

		public bool Unique { get; }
	}
}