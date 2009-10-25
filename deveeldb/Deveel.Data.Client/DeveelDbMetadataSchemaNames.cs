//  
//  DeveelDbMetadataSchemaNames.cs
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

namespace Deveel.Data.Client {
	public sealed class DeveelDbMetadataSchemaNames {
		private DeveelDbMetadataSchemaNames() {
		}

		public static readonly string PrimaryKeys = "PrimaryKeys";
		public static readonly string ImportedKeys = "ImportedKeys";
		public static readonly string ExportedKeys = "ExportedKeys";
		public static readonly string CrossReferences = "CroessReferences";
		public static readonly string Tables = "Tables";
		public static readonly string Columns = "Columns";
		public static readonly string TablePrivileges = "TablePrivileges";
		public static readonly string ColumnPrivileges = "ColumnPrivileges";
		public static readonly string MetadataCollections = System.Data.Common.DbMetaDataCollectionNames.MetaDataCollections;

		public static readonly string DataSourceInformation =
			System.Data.Common.DbMetaDataCollectionNames.DataSourceInformation;

		public static readonly string Restrictions = System.Data.Common.DbMetaDataCollectionNames.Restrictions;
	}
}