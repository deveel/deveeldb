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
	/// <summary>
	/// A helper class for the <see cref="Table.DumpTo"/> method.
	/// </summary>
	/// <remarks>
	/// This provides variables static methods for formating the contents 
	/// of a table and outputting it to an output stream.
	/// </remarks>
	class DumpHelper {
		/// <summary>
		/// Dumps the contents of a table to the given output stream.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="output"></param>
		/// <remarks>
		/// It uses a very simple method to format the text.
		/// </remarks>
		public static void Dump(Table table, TextWriter output) {

			int col_count = table.ColumnCount;

			//    if (table is DataTable) {
			//      DataTable data_tab = (DataTable) table;
			//      output.WriteLine("Total Hits: " + data_tab.TotalHits);
			//      output.WriteLine("File Hits:  " + data_tab.FileHits);
			//      output.WriteLine("Cache Hits: " + data_tab.CacheHits);
			//      output.WriteLine();
			//    }

			output.WriteLine("Table row count: " + table.RowCount);
			output.Write("      ");  // 6 spaces

			// First output the column header.
			for (int i = 0; i < col_count; ++i) {
				output.Write(table.GetResolvedVariable(i).ToString());
				if (i < col_count - 1) {
					output.Write(", ");
				}
			}
			output.WriteLine();

			// Print output the contents of each row
			int row_num = 0;
			IRowEnumerator r_enum = table.GetRowEnumerator();
			while (r_enum.MoveNext() && row_num < 250) {
				// Print the row number
				String num = row_num.ToString();
				int space_gap = 4 - num.Length;
				for (int i = 0; i < space_gap; ++i) {
					output.Write(' ');
				}
				output.Write(num);
				output.Write(": ");

				// Print each cell in the row
				int row_index = r_enum.RowIndex;
				for (int col_index = 0; col_index < col_count; ++col_index) {
					TObject cell = table.GetCell(col_index, row_index);
					output.Write(cell.ToString());
					if (col_index < col_count - 1) {
						output.Write(", ");
					}
				}
				output.WriteLine();

				++row_num;
			}
			output.WriteLine("Finished: " + row_num + "/" + table.RowCount);

		}

	}
}