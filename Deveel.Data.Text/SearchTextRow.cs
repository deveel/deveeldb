// 
//  SearchTextRow.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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
using System.Collections;
using System.Runtime.Serialization;

namespace Deveel.Data.Text {
	/// <summary>
	/// Defines a row stored in the search engine index.
	/// </summary>
	[Serializable]
	public sealed class SearchTextRow : ISerializable {
		#region .ctor
		public SearchTextRow(int rowIndex) {
			this.rowIndex = rowIndex;
		}
		
		private SearchTextRow(SerializationInfo info, StreamingContext context) {
			columns = (Hashtable) info.GetValue("Columns", typeof (Hashtable));
			rowIndex = info.GetInt32("RowIndex");
		}
		#endregion

		#region Fields
		private int rowIndex;
		private Hashtable columns;
		#endregion

		#region Properties
		/// <summary>
		/// Gets the index of the row in the database system coresponding
		/// to the row indexed by the search engine.
		/// </summary>
		public int RowIndex {
			get { return rowIndex; }
		}
		#endregion

		#region Public Methods
		public void SetValue(string columnName, string value) {
			if (columnName == null)
				throw new ArgumentNullException("columnName");
			if (columnName.Length == 0)
				throw new ArgumentException();
			
			if (columns == null)
				columns = new Hashtable();

			columns[columnName] = value;
		}
		
		public string GetValue(string columnName) {
			if (columnName == null)
				throw new ArgumentNullException("columnName");
			if (columnName.Length == 0)
				throw new ArgumentException();

			if (columns == null)
				return null;

			return columns[columnName] as string;
		}
		
		public void GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Columns", columns, typeof (Hashtable));
			info.AddValue("RowIndex", rowIndex);
		}
		#endregion
	}
}