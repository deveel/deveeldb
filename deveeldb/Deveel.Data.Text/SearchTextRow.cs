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