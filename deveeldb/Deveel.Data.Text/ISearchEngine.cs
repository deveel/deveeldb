//  
//  ISearchEngine.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;

namespace Deveel.Data.Text {
	public interface ISearchEngine {
		/// <summary>
		/// Creates an index for the given table having the given layout.
		/// </summary>
		/// <param name="tableName">The fully qualified name of the table
		/// to create the search index for.</param>
		/// <param name="indexInfo">The object describing the index to create.</param>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="tableName">table name</paramref>
		/// is <b>null</b>.
		/// </exception>
		/// <exception cref="ArgumentException">
		/// If an index for the given table with the given name already exists 
		/// in the system or if the given <paramref name="indexInfo"/> is not
		/// a full-text index.
		/// </exception>
		/// <exception cref="InvalidOperationException">
		/// If an error occurred while creating the search index.
		/// </exception>
		void CreateIndex(TableName tableName, DataIndexDef indexInfo);

		/// <summary>
		/// Drops the index for the given table.
		/// </summary>
		/// <param name="tableName">The fully qualified name of the table
		/// to drop the search index for.</param>
		/// <param name="indexName">The name of the index to drop.</param>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="tableName">table name</paramref>
		/// is <b>null</b>.
		/// </exception>
		/// <exception cref="ArgumentException">
		/// If an index for the given table does not exists in the system.
		/// </exception>
		/// <exception cref="InvalidOperationException">
		/// If an error occurred while dropping the search index.
		/// </exception>
		void DropIndex(TableName tableName, string indexName);

		/// <summary>
		/// 
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="indexName"></param>
		/// <param name="row"></param>
		void IndexRow(TableName tableName, string indexName, SearchTextRow row);

		void RemoveRow(TableName tableName, string indexName, int rowIndex);

		void UpdateRow(TableName tableName, string indexName, SearchTextRow row);

		SearchResult Search(TableName tableName, string indexName, string query);
	}
}