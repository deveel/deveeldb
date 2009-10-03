// 
//  GTSQLTypeInfoDataSource.cs
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
using System.Collections;

using Deveel.Math;

namespace Deveel.Data {
	/// <summary>
	/// A <see cref="GTDataSource"/> that models all SQL types available.
	/// </summary>
	/// <remarks>
	/// <b>Note:</b> This is not designed to be a long kept object. It must 
	/// not last beyond the lifetime of a transaction.
	/// </remarks>
	internal class GTSQLTypeInfoDataSource : GTDataSource {
		/// <summary>
		/// The DatabaseConnection object.  Currently this is not used, but it may
		/// be needed in the future if user-defined SQL types are supported.
		/// </summary>
		private DatabaseConnection database;

		/// <summary>
		/// The list of info keys/values in this object.
		/// </summary>
		private ArrayList key_value_pairs;

		/// <summary>
		/// Constant for type_nullable types.
		/// </summary>
		private static readonly BigNumber TYPE_NULLABLE =
						  BigNumber.fromInt(1);

		public GTSQLTypeInfoDataSource(DatabaseConnection connection)
			: base(connection.System) {
			database = connection;
			key_value_pairs = new ArrayList();
		}

		/// <summary>
		/// Adds a type description.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="type"></param>
		/// <param name="precision"></param>
		/// <param name="prefix"></param>
		/// <param name="suffix"></param>
		/// <param name="oops"></param>
		/// <param name="searchable"></param>
		private void AddType(String name, SQLTypes type, int precision,
							 String prefix, String suffix, String oops,
							 bool searchable) {
			key_value_pairs.Add(name);
			key_value_pairs.Add(BigNumber.fromLong((int)type));
			key_value_pairs.Add(BigNumber.fromLong(precision));
			key_value_pairs.Add(prefix);
			key_value_pairs.Add(suffix);
			key_value_pairs.Add(searchable ? BigNumber.fromLong(3) :
											 BigNumber.fromLong(0));
		}

		/// <summary>
		/// Initialize the data source.
		/// </summary>
		/// <returns></returns>
		public GTSQLTypeInfoDataSource Init() {
			AddType("BIT", SQLTypes.BIT, 1, null, null, null, true);
			AddType("BOOLEAN", SQLTypes.BIT, 1, null, null, null, true);
			AddType("TINYINT", SQLTypes.TINYINT, 9, null, null, null, true);
			AddType("SMALLINT", SQLTypes.SMALLINT, 9, null, null, null, true);
			AddType("INTEGER", SQLTypes.INTEGER, 9, null, null, null, true);
			AddType("BIGINT", SQLTypes.BIGINT, 9, null, null, null, true);
			AddType("FLOAT", SQLTypes.FLOAT, 9, null, null, null, true);
			AddType("REAL", SQLTypes.REAL, 9, null, null, null, true);
			AddType("DOUBLE", SQLTypes.DOUBLE, 9, null, null, null, true);
			AddType("NUMERIC", SQLTypes.NUMERIC, 9, null, null, null, true);
			AddType("DECIMAL", SQLTypes.DECIMAL, 9, null, null, null, true);
			AddType("CHAR", SQLTypes.CHAR, 9, "'", "'", null, true);
			AddType("VARCHAR", SQLTypes.VARCHAR, 9, "'", "'", null, true);
			AddType("LONGVARCHAR", SQLTypes.LONGVARCHAR, 9, "'", "'", null, true);
			AddType("DATE", SQLTypes.DATE, 9, null, null, null, true);
			AddType("TIME", SQLTypes.TIME, 9, null, null, null, true);
			AddType("TIMESTAMP", SQLTypes.TIMESTAMP, 9, null, null, null, true);
			AddType("BINARY", SQLTypes.BINARY, 9, null, null, null, false);
			AddType("VARBINARY", SQLTypes.VARBINARY, 9, null, null, null, false);
			AddType("LONGVARBINARY", SQLTypes.LONGVARBINARY, 9, null, null, null, false);
			AddType("OBJECT", SQLTypes.OBJECT, 9, null, null, null, false);

			return this;
		}

		// ---------- Implemented from GTDataSource ----------

		public override DataTableDef DataTableDef {
			get { return DEF_DATA_TABLE_DEF; }
		}

		public override int RowCount {
			get { return key_value_pairs.Count/6; }
		}

		public override TObject GetCellContents(int column, int row) {
			int i = (row * 6);
			switch (column) {
				case 0:  // type_name
					return GetColumnValue(column, key_value_pairs[i]);
				case 1:  // data_type
					return GetColumnValue(column, key_value_pairs[i + 1]);
				case 2:  // precision
					return GetColumnValue(column, key_value_pairs[i + 2]);
				case 3:  // literal_prefix
					return GetColumnValue(column, key_value_pairs[i + 3]);
				case 4:  // literal_suffix
					return GetColumnValue(column, key_value_pairs[i + 4]);
				case 5:  // create_params
					return GetColumnValue(column, null);
				case 6:  // nullable
					return GetColumnValue(column, TYPE_NULLABLE);
				case 7:  // case_sensitive
					return GetColumnValue(column, true);
				case 8:  // searchable
					return GetColumnValue(column, key_value_pairs[i + 5]);
				case 9:  // unsigned_attribute
					return GetColumnValue(column, false);
				case 10:  // fixed_prec_scale
					return GetColumnValue(column, false);
				case 11:  // auto_increment
					return GetColumnValue(column, false);
				case 12:  // local_type_name
					return GetColumnValue(column, null);
				case 13:  // minimum_scale
					return GetColumnValue(column, BigNumber.fromLong(0));
				case 14:  // maximum_scale
					return GetColumnValue(column, BigNumber.fromLong(10000000));
				case 15:  // sql_data_type
					return GetColumnValue(column, null);
				case 16:  // sql_datetype_sub
					return GetColumnValue(column, null);
				case 17:  // num_prec_radix
					return GetColumnValue(column, BigNumber.fromLong(10));
				default:
					throw new ApplicationException("Column out of bounds.");
			}
		}

		// ---------- Overwritten from GTDataSource ----------

		protected override void Dispose() {
			base.Dispose();
			key_value_pairs = null;
			database = null;
		}

		// ---------- Static ----------

		/// <summary>
		/// The data table def that describes this table of data source.
		/// </summary>
		internal static readonly DataTableDef DEF_DATA_TABLE_DEF;

		static GTSQLTypeInfoDataSource() {

			DataTableDef def = new DataTableDef();
			def.TableName = new TableName(Database.SystemSchema, "sUSRSQLTypeInfo");

			// Add column definitions
			def.AddColumn(GetStringColumn("TYPE_NAME"));
			def.AddColumn(GetNumericColumn("DATA_TYPE"));
			def.AddColumn(GetNumericColumn("PRECISION"));
			def.AddColumn(GetStringColumn("LITERAL_PREFIX"));
			def.AddColumn(GetStringColumn("LITERAL_SUFFIX"));
			def.AddColumn(GetStringColumn("CREATE_PARAMS"));
			def.AddColumn(GetNumericColumn("NULLABLE"));
			def.AddColumn(GetBooleanColumn("CASE_SENSITIVE"));
			def.AddColumn(GetNumericColumn("SEARCHABLE"));
			def.AddColumn(GetBooleanColumn("UNSIGNED_ATTRIBUTE"));
			def.AddColumn(GetBooleanColumn("FIXED_PREC_SCALE"));
			def.AddColumn(GetBooleanColumn("AUTO_INCREMENT"));
			def.AddColumn(GetStringColumn("LOCAL_TYPE_NAME"));
			def.AddColumn(GetNumericColumn("MINIMUM_SCALE"));
			def.AddColumn(GetNumericColumn("MAXIMUM_SCALE"));
			def.AddColumn(GetStringColumn("SQL_DATA_TYPE"));
			def.AddColumn(GetStringColumn("SQL_DATETIME_SUB"));
			def.AddColumn(GetNumericColumn("NUM_PREC_RADIX"));

			// Set to immutable
			def.SetImmutable();

			DEF_DATA_TABLE_DEF = def;
		}
	}
}