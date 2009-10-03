// 
//  DataIndexDef.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.IO;

namespace Deveel.Data {
	///<summary>
	/// Represents index meta-information on a table. 
	///</summary>
	/// <remarks>
	/// This information is part of DataIndexSetDef and is stored with 
	/// the contents of a table.
	/// </remarks>
	public class DataIndexDef {
		/// <summary>
		/// The name of this index.
		/// </summary>
		private readonly String index_name;

		/// <summary>
		/// The list of column name that this index represents.  For example, if this
		/// is a composite primary key, this would contain each column name in the
		/// primary key.
		/// </summary>
		private readonly String[] column_names;

		/// <summary>
		/// Returns the index set pointer of this index.  This value is used when
		/// requesting the index from an IIndexSet.
		/// </summary>
		private readonly int index_pointer;

		/// <summary>
		/// The type of Index this is.  Currently only 'BLIST' is supported.
		/// </summary>
		private readonly String index_type;

		/// <summary>
		/// True if this index may only contain unique values.
		/// </summary>
		private readonly bool unique;

		///<summary>
		///</summary>
		///<param name="index_name"></param>
		///<param name="column_names"></param>
		///<param name="index_pointer"></param>
		///<param name="index_type"></param>
		///<param name="unique"></param>
		public DataIndexDef(String index_name, String[] column_names,
							int index_pointer, String index_type, bool unique) {

			this.index_name = index_name;
			this.column_names = (String[])column_names.Clone();
			this.index_pointer = index_pointer;
			this.index_type = index_type;
			this.unique = unique;

		}

		///<summary>
		///</summary>
		///<param name="def"></param>
		public DataIndexDef(DataIndexDef def)
			: this(def.index_name, def.column_names, def.index_pointer, def.index_type,
				 def.unique) {
		}

		///<summary>
		/// Returns the name of this index.
		///</summary>
		public string Name {
			get { return index_name; }
		}

		///<summary>
		/// Returns the column names that make up this index.
		///</summary>
		public string[] ColumnNames {
			get { return column_names; }
		}

		/// <summary>
		/// Returns the pointer to the index in the IIndexSet.
		/// </summary>
		public int Pointer {
			get { return index_pointer; }
		}

		///<summary>
		/// Returns a String that describes the type of index this is.
		///</summary>
		public string Type {
			get { return index_type; }
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
			dout.Write(index_name);
			dout.Write(column_names.Length);
			for (int i = 0; i < column_names.Length; ++i) {
				dout.Write(column_names[i]);
			}
			dout.Write(index_pointer);
			dout.Write(index_type);
			dout.Write(unique);
		}

		/// <summary>
		/// Reads a DataIndexDef from the given <see cref="BinaryReader"/> object.
		/// </summary>
		/// <param name="din"></param>
		/// <returns></returns>
		public static DataIndexDef Read(BinaryReader din) {
			int version = din.ReadInt32();
			if (version != 1) {
				throw new IOException("Don't understand version.");
			}
			String index_name = din.ReadString();
			int sz = din.ReadInt32();
			String[] cols = new String[sz];
			for (int i = 0; i < sz; ++i) {
				cols[i] = din.ReadString();
			}
			int index_pointer = din.ReadInt32();
			String index_type = din.ReadString();
			bool unique = din.ReadBoolean();

			return new DataIndexDef(index_name, cols,
									index_pointer, index_type, unique);
		}
	}
}