// 
//  GTStatisticsDataSource.cs
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

namespace Deveel.Data {
	/// <summary>
	/// An implementation of <see cref="IMutableTableDataSource"/> that 
	/// presents database statistical information.
	/// </summary>
	/// <remarks>
	/// <b>Note:</b> This is not designed to be a long kept object. It must not last
	/// beyond the lifetime of a transaction.
	/// </remarks>
	sealed class GTStatisticsDataSource : GTDataSource {
		/// <summary>
		/// Contains all the statistics information for this session.
		/// </summary>
		private String[] statistics_info;
		/// <summary>
		/// The system database stats.
		/// </summary>
		private Stats stats;

		public GTStatisticsDataSource(DatabaseConnection connection)
			: base(connection.System) {
			stats = connection.Database.Stats;
		}

		/// <summary>
		/// Initialize the data source.
		/// </summary>
		/// <returns></returns>
		public GTStatisticsDataSource Init() {

			lock (stats) {
				/*
				TODO: check this...
				stats.set((int)(Runtime.getRuntime().freeMemory() / 1024),
															  "Runtime.memory.freeKB");
				stats.set((int)(Runtime.getRuntime().totalMemory() / 1024),
															  "Runtime.memory.totalKB");
				*/
				String[] key_set = stats.Keys;
				int glob_length = key_set.Length * 2;
				statistics_info = new String[glob_length];

				for (int i = 0; i < glob_length; i += 2) {
					String key_name = key_set[i / 2];
					statistics_info[i] = key_name;
					statistics_info[i + 1] = stats.StatString(key_name);
				}

			}
			return this;
		}

		// ---------- Implemented from GTDataSource ----------

		public override DataTableDef DataTableDef {
			get { return DEF_DATA_TABLE_DEF; }
		}

		public override int RowCount {
			get { return statistics_info.Length/2; }
		}

		public override TObject GetCellContents(int column, int row) {
			switch (column) {
				case 0:  // stat_name
					return GetColumnValue(column, statistics_info[row * 2]);
				case 1:  // value
					return GetColumnValue(column, statistics_info[(row * 2) + 1]);
				default:
					throw new ApplicationException("Column out of bounds.");
			}
		}

		// ---------- Overwritten from GTDataSource ----------

		protected override void Dispose() {
			base.Dispose();
			statistics_info = null;
			stats = null;
		}

		// ---------- Static ----------

		/// <summary>
		/// The data table def that describes this table of data source.
		/// </summary>
		internal static readonly DataTableDef DEF_DATA_TABLE_DEF;

		static GTStatisticsDataSource() {

			DataTableDef def = new DataTableDef();
			def.TableName = new TableName(Database.SystemSchema, "sUSRDatabaseStatistics");

			// Add column definitions
			def.AddColumn(GetStringColumn("stat_name"));
			def.AddColumn(GetStringColumn("value"));

			// Set to immutable
			def.SetImmutable();

			DEF_DATA_TABLE_DEF = def;
		}
	}
}