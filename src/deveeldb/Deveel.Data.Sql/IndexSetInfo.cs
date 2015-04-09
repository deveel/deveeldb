using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class IndexSetInfo : IEnumerable<IndexInfo> {
		private readonly List<IndexInfo> indexes;

		private IndexSetInfo(ObjectName tableName, IEnumerable<IndexInfo> indexes, bool readOnly) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");

			TableName = tableName;
			this.indexes = new List<IndexInfo>();

			if (indexes != null)
				this.indexes.AddRange(indexes);
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
			return indexes.FindIndex(x => x.IndexName.Equals(name, compare));
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
			return new IndexSetInfo(TableName, indexes.AsReadOnly(), true);
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

		public IndexSetInfo DeserializeFrom(Stream stream) {
			throw new NotImplementedException();
		}
	}
}