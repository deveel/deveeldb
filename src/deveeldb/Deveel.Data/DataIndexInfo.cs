// 
//  Copyright 2010  Deveel
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
using System.IO;

namespace Deveel.Data {
	///<summary>
	/// Represents index meta-information on a table. 
	///</summary>
	/// <remarks>
	/// This information is part of DataIndexSetInfo and is stored with 
	/// the contents of a table.
	/// </remarks>
	public sealed class DataIndexInfo : ICloneable {
		/// <summary>
		/// The name of this index.
		/// </summary>
		private readonly string indexName;

		/// <summary>
		/// The list of column name that this index represents.  For example, if this
		/// is a composite primary key, this would contain each column name in the
		/// primary key.
		/// </summary>
		private readonly string[] columnNames;

		/// <summary>
		/// Returns the index set pointer of this index.  This value is used when
		/// requesting the index from an IIndexSet.
		/// </summary>
		private readonly int indexPointer;

		/// <summary>
		/// The type of Index this is.  Currently only 'BLIST' is supported.
		/// </summary>
		private readonly string indexType;

		/// <summary>
		/// True if this index may only contain unique values.
		/// </summary>
		private readonly bool unique;

		///<summary>
		///</summary>
		///<param name="indexName"></param>
		///<param name="columnNames"></param>
		///<param name="indexPointer"></param>
		///<param name="indexType"></param>
		///<param name="unique"></param>
		public DataIndexInfo(string indexName, string[] columnNames, int indexPointer, string indexType, bool unique) {
			this.indexName = indexName;
			this.columnNames = (string[])columnNames.Clone();
			this.indexPointer = indexPointer;
			this.indexType = indexType;
			this.unique = unique;

		}

		///<summary>
		/// Returns the name of this index.
		///</summary>
		public string Name {
			get { return indexName; }
		}

		///<summary>
		/// Returns the column names that make up this index.
		///</summary>
		public string[] ColumnNames {
			get { return columnNames; }
		}

		/// <summary>
		/// Returns the pointer to the index in the IIndexSet.
		/// </summary>
		public int Pointer {
			get { return indexPointer; }
		}

		///<summary>
		/// Returns a String that describes the type of index this is.
		///</summary>
		public string Type {
			get { return indexType; }
		}

		/// <summary>
		/// Returns true if this is a unique index.
		/// </summary>
		public bool IsUniqueIndex {
			get { return unique; }
		}

		///<summary>
		/// Writes this object to the given <see cref="BinaryWriter"/>.
		///</summary>
		///<param name="dout"></param>
		public void Write(BinaryWriter dout) {
			dout.Write(1);
			dout.Write(indexName);
			dout.Write(columnNames.Length);
			for (int i = 0; i < columnNames.Length; ++i) {
				dout.Write(columnNames[i]);
			}
			dout.Write(indexPointer);
			dout.Write(indexType);
			dout.Write(unique);
		}

		/// <summary>
		/// Reads a DataIndexInfo from the given <see cref="BinaryReader"/> object.
		/// </summary>
		/// <param name="din"></param>
		/// <returns></returns>
		public static DataIndexInfo Read(BinaryReader din) {
			int version = din.ReadInt32();
			if (version != 1)
				throw new IOException("Don't understand version.");

			string indexName = din.ReadString();
			int sz = din.ReadInt32();
			string[] columnNames = new string[sz];
			for (int i = 0; i < sz; ++i) {
				columnNames[i] = din.ReadString();
			}
			int indexPointer = din.ReadInt32();
			string indexType = din.ReadString();
			bool unique = din.ReadBoolean();

			return new DataIndexInfo(indexName, columnNames, indexPointer, indexType, unique);
		}

		object ICloneable.Clone() {
			return Clone();
		}

		public DataIndexInfo Clone() {
			return new DataIndexInfo(indexName, (string[])columnNames.Clone(), indexPointer, indexType, unique);
		}
	}
}