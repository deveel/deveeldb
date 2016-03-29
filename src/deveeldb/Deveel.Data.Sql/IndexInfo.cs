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
using System.IO;
using System.Text;

namespace Deveel.Data.Sql {
	public sealed class IndexInfo {
		public IndexInfo(string indexName, string indexType, string[] columnNames) 
			: this(indexName, indexType, columnNames, false) {
		}

		public IndexInfo(string indexName, string indexType, string[] columnNames, bool isUnique, int offset) {
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
			Offset = offset;
		}

		public IndexInfo(string indexName, string indexType, string[] columnNames, bool isUnique)
			: this(indexName, indexType, columnNames, isUnique, -1) {
		}

		public string IndexName { get; private set; }

		public string IndexType { get; private set; }

		public string[] ColumnNames { get; private set; }

		public bool IsUnique { get; private set; }

		public int Offset { get; internal set; }

		internal void SerializeTo(Stream stream) {
			var writer = new BinaryWriter(stream, Encoding.Unicode);
			writer.Write(2);		// Version
			writer.Write(IndexType);
			writer.Write(IndexName);
			writer.Write(IsUnique ? (byte) 1 : (byte) 0);
			writer.Write(Offset);

			var colCount = ColumnNames.Length;
			writer.Write(colCount);
			for (int i = 0; i < colCount; i++) {
				var colName = ColumnNames[i];
				writer.Write(colName);
			}
		}

		internal static IndexInfo DeserializeFrom(Stream stream) {
			var reader = new BinaryReader(stream, Encoding.Unicode);

			var version = reader.ReadInt32();
			if (version != 2)
				throw new FormatException("Invalid version number for Index-Info");

			var indexType = reader.ReadString();
			var indexName = reader.ReadString();
			var unique = reader.ReadByte() == 1;
			var offset = reader.ReadInt32();

			var colCount = reader.ReadInt32();

			var columnNames = new string[colCount];
			for (int i = 0; i < colCount; i++) {
				var columnName = reader.ReadString();
				columnNames[i] = columnName;
			}

			return new IndexInfo(indexName, indexType, columnNames, unique, offset);
		}
	}
}