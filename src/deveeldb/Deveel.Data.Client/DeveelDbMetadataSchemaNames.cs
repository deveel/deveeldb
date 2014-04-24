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

namespace Deveel.Data.Client {
	public sealed class DeveelDbMetadataSchemaNames {
		private DeveelDbMetadataSchemaNames() {
		}

		public static readonly string DataTypes = "DataTypes";
		public static readonly string Schemata = "Schemata";
		public static readonly string PrimaryKeys = "PrimaryKeys";
		public static readonly string ImportedKeys = "ImportedKeys";
		public static readonly string ExportedKeys = "ExportedKeys";
		public static readonly string CrossReferences = "CroessReferences";
		public static readonly string Tables = "Tables";
		public static readonly string Columns = "Columns";
		public static readonly string TablePrivileges = "TablePrivileges";
		public static readonly string ColumnPrivileges = "ColumnPrivileges";
		public static readonly string MetadataCollections = System.Data.Common.DbMetaDataCollectionNames.MetaDataCollections;
		public static readonly string UserPrivileges = "UserPrivileges";

		public static readonly string DataSourceInformation =
			System.Data.Common.DbMetaDataCollectionNames.DataSourceInformation;

		public static readonly string Restrictions = System.Data.Common.DbMetaDataCollectionNames.Restrictions;
	}
}