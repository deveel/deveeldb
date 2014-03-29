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

using Deveel.Data.DbSystem;

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
		void CreateIndex(TableName tableName, DataIndexInfo indexInfo);

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