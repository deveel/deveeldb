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
using System.Collections;
using System.IO;

namespace Deveel.Data {
	/// <summary>
	/// Represents the meta-data for a set of indexes of a table.
	/// </summary>
	public class DataIndexSetDef {
		/// <summary>
		/// The TableName this index set meta data is for.
		/// </summary>
		private readonly TableName table_name;
		/// <summary>
		/// The list of indexes in the table.
		/// </summary>
		private readonly ArrayList index_list;

		/// <summary>
		/// True if this object is immutable.
		/// </summary>
		private bool immutable;

		///<summary>
		///</summary>
		///<param name="table_name"></param>
		public DataIndexSetDef(TableName table_name) {
			this.table_name = table_name;
			index_list = new ArrayList();
			immutable = false;
		}

		///<summary>
		///</summary>
		///<param name="def"></param>
		public DataIndexSetDef(DataIndexSetDef def) {
			table_name = def.table_name;
			index_list = new ArrayList();
			for (int i = 0; i < def.IndexCount; ++i) {
				index_list.Add(new DataIndexDef(def[i]));
			}
			immutable = false;
		}

		///<summary>
		/// Sets the immutable flag.
		///</summary>
		public void SetImmutable() {
			immutable = true;
		}

		///<summary>
		/// Adds a DataIndexDef to this table.
		///</summary>
		///<param name="def"></param>
		///<exception cref="Exception"></exception>
		public void AddDataIndexDef(DataIndexDef def) {
			if (!immutable) {
				index_list.Add(def);
			} else {
				throw new Exception("Tried to add index to immutable def.");
			}
		}

		/// <summary>
		/// Removes an index from the set at the given position.
		/// </summary>
		/// <param name="i">Pointer to the index to remove.</param>
		/// <exception cref="DatabaseException">
		/// If the set is in read-only mode.
		/// </exception>
		public void RemoveDataIndexDef(int i) {
			if (!immutable) {
				index_list.RemoveAt(i);
			} else {
				throw new Exception("Tried to add index to immutable def.");
			}
		}

		/// <summary>
		/// Gets the number of <see cref="DataIndexDef"/> object in the set.
		/// </summary>
		public int IndexCount {
			get { return index_list.Count; }
		}

		/// <summary>
		/// Gets the index at the given position within the set.
		/// </summary>
		/// <param name="i">Position of the index to get.</param>
		/// <returns>
		/// Returns a <see cref="DataIndexDef"/> located at the position 
		/// indicated by <paramref name="i"/>.
		/// </returns>
		/// <exception cref="System.IndexOutOfRangeException">If the given <paramref name="i"/>
		/// is out of range.</exception>
		public DataIndexDef this[int i] {
			get { return (DataIndexDef) index_list[i]; }
		}

		/// <summary>
		/// Gets the position of a named index within the set.
		/// </summary>
		/// <param name="index_name"></param>
		/// <returns>
		/// Returns an integer pointer to the index with the given <paramref name="index_name"/>
		/// if found, otherwise -1.
		/// </returns>
		/// <exception cref="System.ArgumentNullException">If the given <paramref name="index_name"/>
		/// is <b>null</b>.</exception>
		public int FindIndexWithName(String index_name) {
			int sz = IndexCount;
			for (int i = 0; i < sz; ++i) {
				if (this[i].Name.Equals(index_name)) {
					return i;
				}
			}
			return -1;
		}

		/// <summary>
		/// Finds the index for the given column names.
		/// </summary>
		/// <param name="cols">The column name list to search the index.</param>
		/// <remarks>
		/// This method fails if <paramref name="cols"/> order differs
		/// from the defined order <see cref="DataIndexDef.ColumnNames"/>.
		/// </remarks>
		/// <returns>
		/// Returns an integer pointer to the index for the given <paramref name="cols"/>
		/// if found, otherwise -1.
		/// </returns>
		public int FindIndexForColumns(String[] cols) {
			int sz = IndexCount;
			for (int i = 0; i < sz; ++i) {
				String[] t_cols = this[i].ColumnNames;
				if (t_cols.Length == cols.Length) {
					bool passed = true;
					for (int n = 0; n < t_cols.Length && passed; ++n) {
						if (!t_cols[n].Equals(cols[n])) {
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

		///<summary>
		/// Returns the <see cref="DataIndexDef"/> with the given name or 
		/// null if it couldn't be found.
		///</summary>
		///<param name="index_name"></param>
		///<returns></returns>
		public DataIndexDef IndexWithName(String index_name) {
			int i = FindIndexWithName(index_name);
			if (i != -1) {
				return this[i];
			} else {
				return null;
			}
		}

		/// <summary>
		/// Attempts to resolve the given <paramref name="index_name"/> from the 
		/// index in the set.
		/// </summary>
		/// <param name="index_name">Index name to resolve.</param>
		/// <param name="ignore_case">Indicates if the resolving should be
		/// in case-sensitive mode.</param>
		/// <returns>
		/// Returns a <see cref="System.String"/> for the resolved index name.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If none index or if multiple references found for the given 
		/// <paramref name="index_name"/>.
		/// </exception>
		public String ResolveIndexName(String index_name, bool ignore_case) {
			int sz = IndexCount;
			String found = null;
			for (int i = 0; i < sz; ++i) {
				bool passed;
				String cur_index_name = this[i].Name;
				if (ignore_case) {
					passed = String.Compare(cur_index_name, index_name, true) == 0;
				} else {
					passed = cur_index_name.Equals(index_name);
				}
				if (passed) {
					if (found != null) {
						throw new DatabaseException("Ambigious index name '" +
													index_name + "'");
					}
					found = cur_index_name;
				}
			}
			if (found == null) {
				throw new DatabaseException("Index '" + index_name + "' not found.");
			}
			return found;
		}

		///<summary>
		/// Writes this <see cref="DataIndexSetDef"/> object to the given 
		/// <see cref="BinaryWriter"/>.
		///</summary>
		///<param name="dout"></param>
		public void Write(BinaryWriter dout) {
			dout.Write(1);
			dout.Write(table_name.Schema);
			dout.Write(table_name.Name);
			dout.Write(index_list.Count);
			for (int i = 0; i < index_list.Count; ++i) {
				((DataIndexDef)index_list[i]).Write(dout);
			}
		}

		///<summary>
		/// Reads the <see cref="DataIndexSetDef"/> object from the given 
		/// <see cref="BinaryReader"/>.
		///</summary>
		///<param name="din"></param>
		///<returns></returns>
		///<exception cref="IOException"></exception>
		public static DataIndexSetDef Read(BinaryReader din) {
			int version = din.ReadInt32();
			if (version != 1) {
				throw new IOException("Don't understand version.");
			}
			String schema = din.ReadString();
			String name = din.ReadString();
			int sz = din.ReadInt32();
			DataIndexSetDef index_set =
									 new DataIndexSetDef(new TableName(schema, name));
			for (int i = 0; i < sz; ++i) {
				index_set.AddDataIndexDef(DataIndexDef.Read(din));
			}

			return index_set;
		}
	}
}