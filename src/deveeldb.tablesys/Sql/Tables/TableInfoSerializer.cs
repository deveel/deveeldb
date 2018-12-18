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
using System.IO;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Tables {
	static class TableInfoSerializer {
		public static void Serialize(TableInfo tableInfo, BinaryWriter writer) {
			writer.Write(3);    // Version

			var catName = tableInfo.CatalogName;
			if (catName == null)
				catName = String.Empty;

			writer.Write(catName);
			writer.Write(tableInfo.SchemaName.Name);
			writer.Write(tableInfo.Name);

			var colCount = tableInfo.Columns.Count;
			writer.Write(colCount);
			for (int i = 0; i < colCount; i++) {
				var column = tableInfo.Columns[i];
				ColumnInfoSerializer.Serialize(column, writer);
			}

		}

		#region ColumnInfoSerializer

		static class ColumnInfoSerializer {
			public static void Serialize(ColumnInfo columnInfo, BinaryWriter writer) {
				writer.Write(3);    // Version
				writer.Write(columnInfo.ColumnName);

				SqlTypeSerializer.Serialize(columnInfo.ColumnType, writer);

				writer.Write((byte)0);	// reserved

				if (columnInfo.DefaultValue != null) {
					writer.Write((byte)1);
					SqlExpressionSerializer.Serialize(columnInfo.DefaultValue, writer);
				} else {
					writer.Write((byte)0);
				}
			}
		}

		#endregion
	}
}