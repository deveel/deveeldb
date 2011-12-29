// 
//  Copyright 2010-2011 Deveel
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
using System.Collections.Generic;

namespace Deveel.Data {
	/// <summary>
	/// This class represents a temporary table that is built from data that is 
	/// not related to any underlying <see cref="DataTable"/> object from the database.
	/// </summary>
	/// <remarks>
	/// For example, an aggregate function generates data would be write 
	/// into a <see cref="TemporaryTable"/>.
	/// </remarks>
	public sealed class TemporaryTable : DefaultDataTable {
		/// <summary>
		/// The DataTableInfo object that describes the columns in this table.
		/// </summary>
		private readonly DataTableInfo tableInfo;

		/// <summary>
		/// A list that represents the storage of TObject[] arrays for each row of the table.
		/// </summary>
		private readonly List<TObject[]> tableStorage;

		///<summary>
		///</summary>
		///<param name="database"></param>
		///<param name="name"></param>
		///<param name="fields"></param>
		public TemporaryTable(Database database, String name, DataTableColumnInfo[] fields)
			: base(database) {

			tableStorage = new List<TObject[]>();

			tableInfo = new DataTableInfo(new TableName(null, name));
			foreach (DataTableColumnInfo field in fields) {
				tableInfo.AddColumn(field.Clone());
			}
			tableInfo.IsReadOnly = true;
		}

		/// <summary>
		/// Constructs this <see cref="TemporaryTable"/> based on the 
		/// fields from the given <see cref="Table"/> object.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="based_on"></param>
		public TemporaryTable(String name, Table based_on)
			: base(based_on.Database) {

			tableInfo = based_on.TableInfo.Clone(new TableName(null, name));
			tableInfo.IsReadOnly = true;
		}

		/// <summary>
		/// Constructs this <see cref="TemporaryTable"/> based on the given 
		/// <see cref="Table"/> object.
		/// </summary>
		/// <param name="based_on"></param>
		public TemporaryTable(Table based_on)
			: base(based_on.Database) {

			tableInfo = based_on.TableInfo.Clone();
			tableInfo.IsReadOnly = true;
		}



		/* ====== Methods that are only for TemporaryTable interface ====== */

		/// <summary>
		/// Resolves the given column name (eg 'id' or 'Customer.id' or 
		/// 'default.Customer.id') to a column in this table.
		/// </summary>
		/// <param name="col_name"></param>
		/// <returns></returns>
		private VariableName ResolveToVariable(String col_name) {
			VariableName partial = VariableName.Resolve(col_name);
			return partial;
			//    return partial.ResolveTableName(TableName.Resolve(Name));
		}

		/// <summary>
		/// Creates a new row where cells can be inserted into.
		/// </summary>
		public void NewRow() {
			tableStorage.Add(new TObject[ColumnCount]);
			++row_count;
		}

		///<summary>
		/// Sets the cell in the given column / row to the given value.
		///</summary>
		///<param name="cell"></param>
		///<param name="column"></param>
		///<param name="row"></param>
		public void SetRowCell(TObject cell, int column, int row) {
			TObject[] cells = tableStorage[row];
			cells[column] = cell;
		}

		///<summary>
		/// Sets the cell in the column of the last row of this table to 
		/// the given <see cref="TObject"/>.
		///</summary>
		///<param name="cell"></param>
		///<param name="col_name"></param>
		public void SetRowCell(TObject cell, String col_name) {
			VariableName v = ResolveToVariable(col_name);
			SetRowCell(cell, FindFieldName(v), row_count - 1);
		}

		///<summary>
		/// Sets the cell in the column of the last row of this table to 
		/// the given TObject.
		///</summary>
		///<param name="ob"></param>
		///<param name="col_index"></param>
		///<param name="row"></param>
		public void SetRowObject(TObject ob, int col_index, int row) {
			SetRowCell(ob, col_index, row);
		}

		///<summary>
		/// Sets the cell in the column of the last row of this table to 
		/// the given TObject.
		///</summary>
		///<param name="ob"></param>
		///<param name="col_name"></param>
		public void SetRowObject(TObject ob, String col_name) {
			VariableName v = ResolveToVariable(col_name);
			SetRowObject(ob, FindFieldName(v));
		}

		///<summary>
		/// Sets the cell in the column of the last row of this table to 
		/// the given TObject.
		///</summary>
		///<param name="ob"></param>
		///<param name="col_index"></param>
		public void SetRowObject(TObject ob, int col_index) {
			SetRowObject(ob, col_index, row_count - 1);
		}

		/// <summary>
		/// Copies the cell from the given table (src_col, src_row) to the 
		/// last row of the column specified of this table.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="src_col"></param>
		/// <param name="src_row"></param>
		/// <param name="to_col"></param>
		public void SetCellFrom(Table table, int src_col, int src_row,
								String to_col) {
			VariableName v = ResolveToVariable(to_col);
			TObject cell = table.GetCellContents(src_col, src_row);
			SetRowCell(cell, FindFieldName(v), row_count - 1);
		}

		/// <summary>
		/// Copies the contents of the row of the given Table onto the end of 
		/// this table.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="row"></param>
		/// <remarks>
		/// Only copies columns that exist in both tables.
		/// </remarks>
		public void CopyFrom(Table table, int row) {
			NewRow();

			VariableName[] vars = new VariableName[table.ColumnCount];
			for (int i = 0; i < vars.Length; ++i) {
				vars[i] = table.GetResolvedVariable(i);
			}

			for (int i = 0; i < ColumnCount; ++i) {
				VariableName v = GetResolvedVariable(i);
				String col_name = v.Name;
				try {
					int tcol_index = -1;
					for (int n = 0; n < vars.Length || tcol_index == -1; ++n) {
						if (vars[n].Name.Equals(col_name)) {
							tcol_index = n;
						}
					}
					SetRowCell(table.GetCellContents(tcol_index, row), i, row_count - 1);
				} catch (Exception e) {
					Logger.Error(this, e);
					throw new ApplicationException(e.Message, e);
				}
			}
		}


		/// <summary>
		/// This should be called if you want to perform table operations on 
		/// this TemporaryTable.
		/// </summary>
		/// <remarks>
		/// It should be called *after* all the rows have been set.
		/// It generates SelectableScheme object which sorts the columns of 
		/// the table and lets us execute Table operations on this table.
		/// <b>Note</b> After this method is called, the table must not change 
		/// in any way.
		/// </remarks>
		public void SetupAllSelectableSchemes() {
			BlankSelectableSchemes(1);   // <- blind search
			for (int row_number = 0; row_number < row_count; ++row_number) {
				AddRowToColumnSchemes(row_number);
			}
		}

		/* ====== Methods that are implemented for Table interface ====== */

		/// <inheritdoc/>
		public override DataTableInfo TableInfo {
			get { return tableInfo; }
		}

		/// <inheritdoc/>
		public override TObject GetCellContents(int column, int row) {
			TObject[] cells = tableStorage[row];
			TObject cell = cells[column];
			if (cell == null)
				throw new ApplicationException("NULL cell!  (" + column + ", " + row + ")");

			return cell;
		}

		/// <inheritdoc/>
		public override IRowEnumerator GetRowEnumerator() {
			return new SimpleRowEnumerator(row_count);
		}

		/// <inheritdoc/>
		public override void LockRoot(int lockKey) {
			// We don't need to do anything for temporary tables, because they have
			// no root to Lock.
		}

		/// <inheritdoc/>
		public override void UnlockRoot(int lockKey) {
			// We don't need to do anything for temporary tables, because they have
			// no root to unlock.
		}

		/// <inheritdoc/>
		public override bool HasRootsLocked {
			get {
				// A temporary table _always_ has its roots locked.
				return true;
			}
		}


		// ---------- Static convenience methods ----------

		/// <summary>
		/// Creates a table with a single column with the given name and type.
		/// </summary>
		/// <param name="database"></param>
		/// <param name="columnName"></param>
		/// <param name="c"></param>
		/// <returns></returns>
		internal static TemporaryTable SingleColumnTable(Database database, String columnName, Type c) {
			TType ttype = TType.FromType(c);
			DataTableColumnInfo colInfo = new DataTableColumnInfo(null, columnName, ttype);
			TemporaryTable table = new TemporaryTable(database, "single", new DataTableColumnInfo[] { colInfo });

			//      int type = TypeUtil.ToDbType(c);
			//      TableField[] fields =
			//                 { new TableField(columnName, type, Integer.MAX_VALUE, false) };
			//      TemporaryTable table = new TemporaryTable(database, "single", fields);
			return table;
		}

	}
}