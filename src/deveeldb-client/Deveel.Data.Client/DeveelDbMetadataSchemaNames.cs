using System;

namespace Deveel.Data.Client {
	public 
#if NET_2_0
		static
#else
		sealed 
#endif
		class DeveelDbMetadataSchemaNames {
#if !NET_2_0
		private DeveelDbMetadataSchemaNames() {
		}
#endif

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

		public static readonly string DataSourceInformation =
			System.Data.Common.DbMetaDataCollectionNames.DataSourceInformation;

		public static readonly string Restrictions = System.Data.Common.DbMetaDataCollectionNames.Restrictions;
	}
}