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
using System.IO;
using System.Text;

namespace Deveel.Data.Sql {
	public sealed class IndexInfo {
		public IndexInfo(string indexName, string indexType, string[] columnNames) 
			: this(indexName, indexType, columnNames, false) {
		}

		public IndexInfo(string indexName, string indexType, string[] columnNames, bool isUnique) {
			if (String.IsNullOrEmpty(indexType))
				throw new ArgumentNullException("indexType");
			if (String.IsNullOrEmpty(indexName))
				throw new ArgumentNullException("indexName");
			if (columnNames == null || columnNames.Length == 0)
				throw new ArgumentNullException("columnNames");

			IndexName = indexName;
			IndexType = indexType;
			ColumnNames = columnNames;
			IsUnique = isUnique;
		}

		public string IndexName { get; private set; }

		public string IndexType { get; private set; }

		public string[] ColumnNames { get; private set; }

		public bool IsUnique { get; private set; }

		public int Offset { get; internal set; }

		public void SerializeTo(Stream stream) {
			var writer = new BinaryWriter(stream, Encoding.Unicode);
			writer.Write(2);		// Version
			writer.Write(IndexType);
			writer.Write(IndexName);
			writer.Write(IsUnique ? 1 : 0);
			writer.Write(Offset);

			var colCount = ColumnNames.Length;
			writer.Write(colCount);
			for (int i = 0; i < colCount; i++) {
				var colName = ColumnNames[i];
				writer.Write(colName);
			}
		}
	}
}