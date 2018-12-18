using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Deveel.Data.Sql.Indexes {
	static class IndexSetInfoSerializer {
		public static void Serialize(IndexSetInfo indexSetInfo, BinaryWriter writer) {
			var schemaName = indexSetInfo.TableName.Parent;
			var catName = schemaName != null && schemaName.Parent != null ? schemaName.Parent.Name : String.Empty;
			var schema = schemaName != null ? schemaName.Name : String.Empty;

			writer.Write(2);		// Version
			writer.Write(catName);
			writer.Write(schema);
			writer.Write(indexSetInfo.TableName.Name);

			int indexCount = indexSetInfo.Indexes.Count;

			writer.Write(indexCount);

			for (int i = 0; i < indexCount; i++) {
				var index = indexSetInfo.Indexes[i];
				IndexInfoSerializer.Serialize(index, writer);
			}
		}

		#region IndexInfoSerializer

		static class IndexInfoSerializer {
			public static void Serialize(IndexInfo indexInfo, BinaryWriter writer) {
				writer.Write(2);		// Version
				// TODO: writer.Write(indexInfo.IndexType);
				writer.Write(indexInfo.IndexName);
				writer.Write(indexInfo.Unique ? (byte) 1 : (byte) 0);
				// TODO: writer.Write(Offset);

				var colCount = indexInfo.ColumnNames.Length;
				writer.Write(colCount);
				for (int i = 0; i < colCount; i++) {
					var colName = indexInfo.ColumnNames[i];
					writer.Write(colName);
				}
			}
		}

		#endregion
	}
}