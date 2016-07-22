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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Deveel.Data.Sql {
	public sealed class IndexSetInfo : IEnumerable<IndexInfo> {
		private readonly List<IndexInfo> indexes;

		private IndexSetInfo(ObjectName tableName, IEnumerable<IndexInfo> indexes, bool readOnly) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");

			TableName = tableName;
			this.indexes = new List<IndexInfo>();

			if (indexes != null)
				this.indexes.AddRange(indexes);

			IsReadOnly = readOnly;
		}

		public IndexSetInfo(ObjectName tableName)
			: this(tableName, null, false) {
		}

		public ObjectName TableName { get; private set; }

		public bool IsReadOnly { get; private set; }

		public int IndexCount {
			get { return indexes.Count; }
		}

		private void AssertNotReadOnly() {
			if (IsReadOnly)
				throw new ArgumentException("The set is read-only.");
		}

		public void AddIndex(IndexInfo indexInfo) {
			AssertNotReadOnly();

			indexes.Add(indexInfo);
			indexInfo.Offset = indexes.Count - 1;
		}

		public IndexInfo GetIndex(int offset) {
			return indexes.FirstOrDefault(x => x.Offset == offset);
		}

		public void RemoveIndexAt(int offset) {
			AssertNotReadOnly();

			indexes.RemoveAt(offset);
		}

		public IndexInfo FindNamedIndex(string indexName) {
			return FindNamedIndex(indexName, true);
		}

		public IndexInfo FindNamedIndex(string indexName, bool ignoreCase) {
			var compare = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
			return indexes.FirstOrDefault(x => x.IndexName.Equals(indexName, compare));
		}

		public int FindIndexOffset(string name) {
			return FindIndexOffset(name, true);
		}

		public int FindIndexOffset(string name, bool ignoreCase) {
			var compare = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
			for (int i = 0; i < indexes.Count; i++) {
				var indexInfo = indexes[i];
				if (indexInfo.IndexName.Equals(name, compare))
					return i;
			}

			return -1;
		}

		public int FindIndexForColumns(string[] columnNames) {
			int sz = IndexCount;
			for (int i = 0; i < sz; ++i) {
				string[] columns = indexes[i].ColumnNames;
				if (columns.Length == columnNames.Length) {
					bool passed = true;
					for (int n = 0; n < columns.Length && passed; ++n) {
						if (!columns[n].Equals(columnNames[n])) {
							passed = false;
						}
					}
					if (passed) {
						return i;
					}
				}
			}
			return -1;
		}


		public IndexSetInfo AsReadOnly() {
			return new IndexSetInfo(TableName, indexes.ToArray(), true);
		}

		public IEnumerator<IndexInfo> GetEnumerator() {
			return indexes.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public void SerialiazeTo(Stream stream) {
			var schemaName = TableName.Parent;
			var catName = schemaName != null && schemaName.Parent != null ? schemaName.Parent.Name : String.Empty;
			var schema = schemaName != null ? schemaName.Name : String.Empty;

			var writer = new BinaryWriter(stream, Encoding.Unicode);
			writer.Write(2);		// Version
			writer.Write(catName);
			writer.Write(schema);
			writer.Write(TableName.Name);

			int indexCount = indexes.Count;

			writer.Write(indexCount);

			for (int i = 0; i < indexCount; i++) {
				var index = indexes[i];
				index.SerializeTo(stream);
			}
		}

		public static IndexSetInfo DeserializeFrom(Stream stream) {
			var reader = new BinaryReader(stream, Encoding.Unicode);

			var version = reader.ReadInt32();
			if (version != 2)
				throw new FormatException("Invalid version number of the Index-Set Info");

			var catName = reader.ReadString();
			var schemaName = reader.ReadString();
			var tableName = reader.ReadString();

			ObjectName objSchemaName;
			if (String.IsNullOrEmpty(catName)) {
				objSchemaName = new ObjectName(schemaName);
			} else {
				objSchemaName = new ObjectName(new ObjectName(catName), schemaName);
			}

			var objTableName = new ObjectName(objSchemaName, tableName);

			var indexCount = reader.ReadInt32();

			var indices = new List<IndexInfo>();
			for (int i = 0; i < indexCount; i++) {
				var indexInfo = IndexInfo.DeserializeFrom(stream);
				indices.Add(indexInfo);
			}

			return new IndexSetInfo(objTableName, indices.ToArray(), false);
		}
	}
}