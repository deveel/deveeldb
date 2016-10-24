using System;

using Deveel.Data.Build;
using Deveel.Data.Security;
using Deveel.Data.Services;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Schemas {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseSchemaFeature(this ISystemBuilder builder) {
			return builder.UseFeature(feature => feature.Named(SystemFeatureNames.Schemata)
				.WithAssemblyVersion()
				.OnSystemBuild(OnBuild)
				.OnTableCompositeCreate(OnCompositeCreate)
				.OnDatabaseCreate(OnDatabaseCreate));
		}

		private static void OnDatabaseCreate(IQuery systemQuery) {
			if (systemQuery.Access().HasSecurity) {
				// This view shows the grants that the user has (no join, only priv_bit).
				systemQuery.CreateView(InformationSchema.ThisUserSimpleGrantViewName, query => query
					.Column("priv_bit")
					.Column("object")
					.Column("name")
					.Column("grantee")
					.Column("grant_option")
					.Column("granter")
					.FromTable(SystemSchema.GrantsTableName)
					.Where(where => where.Reference("grantee").Equal(right => right.Function("user"))
						.Or(or => or.Reference("grantee").Equal(right => right.Value(User.PublicName)))));

				// This view shows the grants that the user is allowed to see
				// CREATE VIEW ThisUserGrant AS
				//     SELECT i_privilege_string(priv_bit) AS description, object, name, grantee, grant_option, granter
				//         FROM grants
				//         WHERE (grantee = user() OR grantee = 'PUBLIC')

				systemQuery.CreateView(InformationSchema.ThisUserGrantViewName, query => query
					.Function("i_privilege_string", new SqlExpression[] {
						SqlExpression.Reference(new ObjectName("priv_bit"))
					}, "description")
					.Column("object")
					.Column("name")
					.Column("grantee")
					.Column("grant_option")
					.Column("granter")
					.FromTable(SystemSchema.GrantsTableName)
					.Where(where => where.Reference("grantee").Equal(right => right.Function("user"))
						.Or(or => or.Reference("grantee").Equal(right => right.Value(User.PublicName)))));

				// A view that represents the list of schema this user is allowed to view
				// the contents of.

				//  CREATE VIEW ThisUserSchemaInfo AS
				//      SELECT * FROM  SYSTEM.schema_info
				//          WHERE name IN (
				//              SELECT name
				//                  FROM INFORMATION_SCHEMA.ThisUserGrant
				//                  WHERE object = 1 AND description LIKE '%LIST%')

				systemQuery.CreateView(InformationSchema.ThisUserSchemaInfoViewName, query => query
					.AllColumns()
					.FromTable(SystemSchema.SchemaInfoTableName)
					.Where(where => where
						.Reference("name")
						.In(@in => @in
							.Query(sub => sub
								.Column("name")
								.FromTable(InformationSchema.ThisUserGrantViewName)
								.Where(filter => filter
									.Reference("object").Equal(value => value.Value((int) DbObjectType.Schema))
									.And(and => and
										.Reference("description")
										.Like(String.Format("%{0}%", Privileges.List.ToString().ToUpperInvariant()))))))));

				// A view that exposes the table_columns table but only for the tables
				// this user has read access to.

				// CREATE VIEW INFORMATION_SCHEMA.ThisUserTableColumns AS
				//     SELECT * FROM SYSTEM.table_columns
				//         WHERE schema IN (
				//             SELECT name FROM INFORMATION_SCHEMA.ThisUserSchemaInfo)

				systemQuery.CreateView(InformationSchema.ThisUserTableColumnsViewName, query => query
					.AllColumns()
					.FromTable(SystemSchema.TableColumnsTableName)
					.Where(where => where
						.Reference("schema")
						.In(@in => @in
							.Query(sub => sub
								.Column("name")
								.FromTable(InformationSchema.ThisUserSchemaInfoViewName)))));

				// A view that exposes the 'table_info' table but only for the tables
				// this user has read access to.

				// CREATE VIEW INFORMATION_SCHEMA.ThisUserTableInfo AS
				//    SELECT * FROM SYSTEM.table_info
				//        WHERE schema IN (
				//            SELECT name FROM INFORMATION_SCHEMA.ThisUserSchemaInfo)

				systemQuery.CreateView(InformationSchema.ThisUserTableInfoViewName, query => query
					.AllColumns()
					.FromTable(SystemSchema.TableInfoTableName)
					.Where(where => where
						.Reference("schema")
						.In(@in => @in
							.Query(sub => sub
								.Column("name")
								.FromTable(InformationSchema.ThisUserSchemaInfoViewName)))));

				// CREATE VIEW INFORMATION_SCHEMA.Tables AS
				//     SELECT NULL AS TABLE_CATALOG,
				//            schema AS TABLE_SCHEMA,
				//            name AS TABLE_NAME
				//            type AS TABLE_TYPE,
				//            other AS REMARKS
				//            NULL AS TYPE_CATALOG
				//            NULL AS TYPE_SCHEMA
				//            NULL AS TYPE_NAME
				//            NULL AS SELF_REFERENCING_COL_NAME
				//            NULL AS REF_GENERATION
				//    FROM INFORMATION_SCHEMA.ThisUserTableInfo

				systemQuery.CreateView(InformationSchema.Tables, query => query
					.Constant(null, "TABLE_CATALOG")
					.Column("schema", "TABLE_SCHEMA")
					.Column("name", "TABLE_NAME")
					.Column("type", "TABLE_TYPE")
					.Column("other", "REMARKS")
					.Constant(null, "TYPE_CATALOG")
					.Constant(null, "TYPE_SCHEMA")
					.Constant(null, "TYPE_NAME")
					.Constant(null, "SELF_REFERENCING_COL_NAME")
					.Constant(null, "REF_GENERATION")
					.FromTable(InformationSchema.ThisUserTableInfoViewName));

				// CREATE VIEW INFORMATION_SCHEMA.Schemata AS
				//    SELECT name AS TABLE_SCHEMA
				//           NULL AS TABLE_CATALOG
				//    FROM INFORMATION_SCHEMA.ThisUserSchemaInfo

				systemQuery.CreateView(InformationSchema.Schemata, query => query
					.Column("name", "TABLE_SCHEMA")
					.Constant(null, "TABLE_CATALOG")
					.FromTable(InformationSchema.ThisUserSchemaInfoViewName));

				// CREATE VIEW INFORMATION_SCHEMA.Catalogs AS
				//    SELECT NULL AS TABLE_CATALOG
				//        FROM SYSTEM.schema_info
				//        WHERE FALSE
				 
				systemQuery.CreateView(InformationSchema.Catalogs, query => query
					.Constant(null, "TABLE_CATALOG")
					.FromTable(SystemSchema.SchemaInfoTableName)
					.Where(SqlExpression.Constant(false))); // Hacky, this will generate a 0 row

				//  CREATE VIEW INFORMATION_SCHEMA.Columns AS
				//      SELECT NULL AS TABLE_CATALOG,
				//             schema AS TABLE_SCHEMA,
				//             table AS TABLE_NAME,
				//             column AS COLUMN_NAME,
				//	           sql_type AS DATA_TYPE,
				//             type_desc AS TYPE_NAME,
				//             IIF(size = -1, 1024, size) AS COLUMN_SIZE,
				//             NULL AS BUFFER_LENGTH,
				//             scale AS DECIMAL_DIGITS,
				//             IIF(sql_type = -7, 2, 10) AS NUM_PREC_RADIX,
				//             IIF(not_null, 0, 1) AS NULLABLE,
				//             '' AS REMARKS,
				//             default AS COLUMN_DEFAULT,
				//             NULL AS SQL_DATA_TYPE,
				//             NULL AS SQL_DATETIME_SUB,
				//             IIF(size = -1, 1024, size) AS CHAR_OCTET_LENGTH,
				//             seq_no + 1 AS ORDINAL_POSITION,
				//             IIF(not_null, 'NO', 'YES') AS IS_NULLABLE
				//      FROM INFORMATION_SCHEMA.ThisUserTableColumns

				systemQuery.CreateView(InformationSchema.Columns, query => query
					.Constant(null, "TABLE_CATALOG")
					.Column("schema", "TABLE_SCHEMA")
					.Column("table", "TABLE_NAME")
					.Column("column", "COLUMN_NAME")
					.Column("sql_type", "DATA_TYPE")
					.Column("type_desc", "TYPE_NAME")
					.Function("IIF", new SqlExpression[] {
						SqlExpression.Equal(SqlExpression.Reference(new ObjectName("size")), SqlExpression.Constant(-1)),
						SqlExpression.Constant(1024),
						SqlExpression.Reference(new ObjectName("size"))
					}, "COLUMN_SIZE")
					.Constant(null, "BUFFER_LENGTH")
					.Column("scale", "DECIMAL_DIGITS")
					.Function("IIF", new SqlExpression[] {
						SqlExpression.Equal(SqlExpression.Reference(new ObjectName("sql_type")),
							SqlExpression.Constant((int) SqlTypeCode.Float)),
						SqlExpression.Constant(2),
						SqlExpression.Constant(10)
					}, "NUM_PREC_RADIX")
					.Function("IIF", new SqlExpression[] {
						SqlExpression.Reference(new ObjectName("not_null")),
						SqlExpression.Constant(0),
						SqlExpression.Constant(1),
					}, "NULLABLE")
					.Constant(String.Empty, "REMARKS")
					.Column("default", "COLUMN_DEFAULT")
					.Constant(null, "SQL_DATA_TYPE")
					.Constant(null, "SQL_DATETIME_SUB")
					.Function("IIF", new SqlExpression[] {
						SqlExpression.Equal(SqlExpression.Reference(new ObjectName("size")), SqlExpression.Constant(-1)),
						SqlExpression.Constant(1024),
						SqlExpression.Reference(new ObjectName("size"))
					}, "CHAR_OCTET_LENGTH")
					.Expression(SqlExpression.Add(SqlExpression.Reference(new ObjectName("seq_no")), SqlExpression.Constant(1)),
						"ORDINAL_POSITION")
					.Function("IIF", new SqlExpression[] {
						SqlExpression.Reference(new ObjectName("not_null")),
						SqlExpression.Constant("NO"),
						SqlExpression.Constant("YES"),
					}, "IS_NULLABLE")
					.FromTable(InformationSchema.ThisUserTableColumnsViewName));

				// CREATE VIEW INFORMATION_SCHEMA.Column_Privileges AS 
				//     SELECT TABLE_CATALOG,
				//            TABLE_SCHEMA,
				//            TABLE_NAME,
				//            COLUMN_NAME,
				//            IIF(INFORMATION_SCHEMA.ThisUserGrant.granter = '@SYSTEM', NULL, INFORMATION_SCHEMA.ThisUserGrant.granter) AS GRANTOR,
				//            IIF(INFORMATION_SCHEMA.ThisUserGrant.grantee = 'PUBLIC', 'public', INFORMATION_SCHEMA.ThisUserGrant.grantee) AS GRANTEE,
				//            INFORMATION_SCHEMA.ThisUserGrant.description AS PRIVILEGE,
				//            IIF(grant_option, 'YES', 'NO') AS IS_GRANTABLE
				//    FROM INFROMATION_SCHEMA.Columns, INFORMATION_SCHEMA.ThisUserGrant
				//    WHERE CONCAT(columns.TABLE_SCHEMA, '.', columns.TABLE_NAME) = ThisUserGrant.name
				//        AND INFORMATION_SCHEMA.ThisUserGrant.object = 1 
				//        AND INFORMATION_SCHEMA.ThisUserGrant.description IS NOT NULL

				systemQuery.CreateView(InformationSchema.ColumnPrivileges, query => query
					.Column("TABLE_CATALOG")
					.Column("TABLE_SCHEMA")
					.Column("TABLE_NAME")
					.Column("COLUMN_NAME")
					.Item(item => item.Expression(exp => exp
							.Function("IIF",
								a => a.Reference(InformationSchema.ThisUserGrantViewName, "granter")
									.Equal(eq => eq.Value(User.SystemName)),
								b => b.Value(null),
								c => c.Reference(InformationSchema.ThisUserGrantViewName, "granter")))
						.As("GRANTOR"))
					.Item(item => item.Expression(exp => exp
						.Function("IIF",
							a => a.Reference(InformationSchema.ThisUserGrantViewName, "grantee").Equal(eq => eq.Value(User.PublicName)),
							b => b.Value("public"),
							c => c.Reference(InformationSchema.ThisUserGrantViewName, "grantee"))).As("GRANTEE"))
					.Function("IIF", new SqlExpression[] {
						SqlExpression.Reference(new ObjectName("grant_option")),
						SqlExpression.Constant("YES"),
						SqlExpression.Constant("NO")
					}, "IS_GRANTABLE")
					.From(source => source.Table(InformationSchema.Columns),
						source => source.Table(InformationSchema.ThisUserGrantViewName))
					.Where(where => where
						.Function("CONCAT",
							a => a.Reference(InformationSchema.Columns, "TABLE_SCHEMA"),
							b => b.Value("."),
							c => c.Reference(InformationSchema.Columns, "TABLE_NAME"))
						.Equal(eq => eq.Reference(InformationSchema.ThisUserGrantViewName, "name"))
						.And(and => and
							.Reference(InformationSchema.ThisUserGrantViewName, "object")
							.Equal(eq => eq.Value((int) DbObjectType.Table)))
						.And(and => and
							.Reference(InformationSchema.ThisUserGrantViewName, "description")
							.IsNot(@is => @is.Value(null)))));

				// CREATE VIEW INFORMATION_SCHEMA.Table_Privileges AS
				//   SELECT TABLE_CATALOG,
				//          TABLE_SCHEMA,
				//          TABLE_NAME,
				//          IIF(INFORMATION_SCHEMA.ThisUserGrant.granter = '@SYSTEM', NULL, INFORMATION_SCHEMA.ThisUserGrant.granter) AS GRANTOR,
				//          IIF(INFORMATION_SCHEMA.ThisUserGrant.grantee = 'PUBLIC', 'public', INFORMATION_SCHEMA.ThisUserGrant.grantee) AS GRANTEE,
				//          INFORMATION_SCHEMA.ThisUserGrant.description AS PRIVILEGE,
				//          IIF(grant_option, 'YES', 'NO') AS IS_GRANTABLE
				//    FROM INFORMATION_SCHEMA.Tables, INFORMATION_SCHEMA.ThisUserGrantViewName
				//    WHERE CONCAT(tables.TABLE_SCHEMA, '.', tables.TABLE_NAME) = INFORMATION_SCHEMA.ThisUserGrant.name
				//     AND INFORMATION_SCHEMA.ThisUserGrant.object = 1
				//     AND INFORMATION_SCHEMA.ThisUserGrant.description IS NOT NULL

				systemQuery.CreateView(InformationSchema.TablePrivileges, query => query
					.Column("TABLE_CATALOG")
					.Column("TABLE_SCHEMA")
					.Column("TABLE_NAME")
					.Function("IIF", new SqlExpression[] {
						SqlExpression.Equal(
							SqlExpression.Reference(new ObjectName(InformationSchema.ThisUserGrantViewName, "granter")),
							SqlExpression.Constant(User.SystemName)),
						SqlExpression.Constant(null),
						SqlExpression.Reference(new ObjectName(InformationSchema.ThisUserGrantViewName, "granter"))
					}, "GRANTOR")
					.Item(item => item.Expression(exp => exp
						.Function("IIF",
							a => a.Reference(InformationSchema.ThisUserGrantViewName, "grantee").Equal(eq => eq.Value(User.PublicName)),
							b => b.Value("public"),
							c => c.Reference(InformationSchema.ThisUserGrantViewName, "grantee"))).As("GRANTEE"))
					.Function("IIF", new SqlExpression[] {
						SqlExpression.Reference(new ObjectName("grant_option")),
						SqlExpression.Constant("YES"),
						SqlExpression.Constant("NO")
					}, "IS_GRANTABLE")
					.Column(new ObjectName(InformationSchema.ThisUserGrantViewName, "description"), "PRIVILEGE")
					.FromTable(InformationSchema.Tables)
					.FromTable(InformationSchema.ThisUserGrantViewName)
					.Where(where => where.Function("CONCAT",
							a => a.Reference(InformationSchema.Tables, "TABLE_SCHEMA"),
							b => b.Value("."),
							c => c.Reference(InformationSchema.Tables, "TABLE_NAME"))
						.Equal(eq => eq.Reference(InformationSchema.ThisUserGrantViewName, "name"))
						.And(and => and
							.Reference(InformationSchema.ThisUserGrantViewName, "object")
							.Equal(eq => eq.Value((int) DbObjectType.Table)))
						.And(and => and
							.Reference(InformationSchema.ThisUserGrantViewName, "description")
							.IsNot(@is => @is.Value(null)))));

				// CREATE VIEW INFORMATION_SCHEMA.Primary_Keys AS 
				//   SELECT NULL AS TABLE_CATALOG,
				//          schema AS TABLE_SCHEMA,
				//          table AS TABLE_NAME,
				//          column AS COLUMN_NAME,
				//          SYSTEM.pkey_cols.seq_no AS KEY_SEQ,
				//          name AS PK_NAME
				//   FROM SYSTEM.pkey_info, SYSTEM_pkey_cols
				//	 WHERE pkey_info.id = pkey_cols.pk_id
				//     AND schema IN ( SELECT name FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )

				systemQuery.CreateView(InformationSchema.PrimaryKeys, query => query
					.Constant(null, "TABLE_CATALOG")
					.Column("schema", "TABLE_SCHEMA")
					.Column("table", "TABLE_NAME")
					.Column("column", "COLUMN_NAME")
					.Column(new ObjectName(SystemSchema.PrimaryKeyColumnsTableName, "seq_no"), "KEY_SEQ")
					.Column("name", "PK_NAME")
					.FromTable(SystemSchema.PrimaryKeyInfoTableName)
					.FromTable(SystemSchema.PrimaryKeyColumnsTableName)
					.Where(where => where
						.Reference("pkey_info", "id")
						.Equal(eq => eq.Reference("pkey_cols", "pk_id"))
						.And(and => and
							.Reference("schema")
							.In(@in => @in
								.Query(sub => sub
									.Column("name")
									.FromTable(InformationSchema.ThisUserSchemaInfoViewName))))));

				// CREATE VIEW INFORMATION_SCHEMA.Imported_Keys AS
				//   SELECT NULL AS PKTABLE_CATALOG,
				//          fkey_info.ref_schema AS PKTABLE_SCHEMA,
				//          fkey_info.ref_table AS PKTABLE_NAME,
				//          fkey_cols.pcolumn AS PKCOLUMN_NAME,
				//          NULL AS FKTABLE_CATALOG,
				//          fkey_info.schema AS FKTABLE_SCHEMA,
				//          fkey_info.table AS FKTABLE_NAME,
				//          fkey_cols.fcolumn AS FKCOLUMN_NAME,
				//          fkey_cols.seq_no AS KEY_SEQ,
				//          I_FRULE_CONVERT(fkey_info.update_rule) AS UPDATE_RULE,
				//          I_FRULE_CONVERT(fkey_info.delete_rule) AS DELETE_RULE,
				//          fkey_info.name AS FK_NAME,
				//          NULL AS PK_NAME,
				//          fkey_info.deferred AS DEFERRABILITY
				//   FROM SYSTEM.fkey_info, SYSTEM.fkey_cols
				//   WHERE fkey_info.id = fkey_cols.fk_id
				//     AND fkey_info.schema IN ( SELECT name FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )

				systemQuery.CreateView(InformationSchema.ImportedKeys, query => query
					.Constant(null, "PKTABLE_CATALOG")
					.Column(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "ref_schema"), "PKTABLE_SCHEMA")
					.Column(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "ref_table"), "PKTABLE_NAME")
					.Column(new ObjectName(SystemSchema.ForeignKeyColumnsTableName, "pcolumn"), "PKCOLUMN_NAME")
					.Constant(null, "FKTABLE_CATALOG")
					.Column(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "schema"), "FKTABLE_SCHEMA")
					.Column(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "table"), "FKTABLE_NAME")
					.Column(new ObjectName(SystemSchema.ForeignKeyColumnsTableName, "fcolumn"), "FKCOLUMN_NAME")
					.Column(new ObjectName(SystemSchema.ForeignKeyColumnsTableName, "seq_no"), "KEY_SEQ")
					.Function("I_FRULE_CONVERT",
						new SqlExpression[] {SqlExpression.Reference(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "update_rule"))},
						"UPDATE_RULE")
					.Function("I_FRULE_CONVERT",
						new SqlExpression[] {SqlExpression.Reference(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "delete_rule"))},
						"DELETE_RULE")
					.Column(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "name"), "FK_NAME")
					.Constant(null, "PK_NAME")
					.Column(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "deferred"), "DEFERRABILITY")
					.From(source => source.Table(SystemSchema.ForeignKeyInfoTableName),
						source => source.Table(SystemSchema.ForeignKeyColumnsTableName))
					.Where(where =>
						where.Reference(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "id"))
							.Equal(eq => eq.Reference(new ObjectName(SystemSchema.ForeignKeyColumnsTableName, "fk_id")))
							.And(and => and.Reference(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "schema"))
								.In(@in => @in
									.Query(q => q
										.Column("name")
										.FromTable(InformationSchema.ThisUserSchemaInfoViewName))))));

				// CREATE VIEW INFORMATION_SCHEMA.Exported_Keys AS
				//   SELECT NULL AS PKTABLE_CATALOG,
				//          fkey_info.ref_schema AS PKTABLE_SCHEMA,
				//          fkey_info.ref_table AS PKTABLE_NAME,
				//          fkey_cols.pcolumn AS PKCOLUMN_NAME,
				//          NULL AS FKTABLE_CATALOG,
				//          fkey_info.schema AS FKTABLE_SCHEMA,
				//          fkey_info.table AS FKTABLE_NAME,
				//          fkey_cols.fcolumn AS FKCOLUMN_NAME,
				//          fkey_cols.seq_no AS KEY_SEQ,
				//          I_FRULE_CONVERT(fkey_info.update_rule) AS UPDATE_RULE,
				//          I_FRULE_CONVERT(fkey_info.delete_rule) AS DELETE_RULE,
				//          fkey_info.name AS FK_NAME,
				//          NULL AS PK_NAME,
				//          fkey_info.deferred AS DEFERRABILITY
				//   FROM SYSTEM.fkey_info, SYSTEM.fkey_cols
				//   WHERE fkey_info.id = fkey_cols.fk_id
				//     AND fkey_info.schema IN ( SELECT name FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )

				systemQuery.CreateView(InformationSchema.ExportedKeys, query => query
					.Constant(null, "PKTABLE_CATALOG")
					.Column(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "ref_schema"), "PKTABLE_SCHEMA")
					.Column(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "ref_table"), "PKTABLE_NAME")
					.Column(new ObjectName(SystemSchema.ForeignKeyColumnsTableName, "pcolumn"), "PKCOLUMN_NAME")
					.Constant(null, "FKTABLE_CATALOG")
					.Column(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "schema"), "FKTABLE_SCHEMA")
					.Column(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "table"), "FKTABLE_NAME")
					.Column(new ObjectName(SystemSchema.ForeignKeyColumnsTableName, "fcolumn"), "FKCOLUMN_NAME")
					.Column(new ObjectName(SystemSchema.ForeignKeyColumnsTableName, "seq_no"), "KEY_SEQ")
					.Function("I_FRULE_CONVERT",
						new SqlExpression[] {SqlExpression.Reference(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "update_rule"))},
						"UPDATE_RULE")
					.Function("I_FRULE_CONVERT",
						new SqlExpression[] {SqlExpression.Reference(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "delete_rule"))},
						"DELETE_RULE")
					.Column(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "name"), "FK_NAME")
					.Constant(null, "PK_NAME")
					.Column(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "deferred"), "DEFERRABILITY")
					.From(source => source.Table(SystemSchema.ForeignKeyInfoTableName),
						source => source.Table(SystemSchema.ForeignKeyColumnsTableName))
					.Where(where =>
						where.Reference(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "id"))
							.Equal(eq => eq.Reference(new ObjectName(SystemSchema.ForeignKeyColumnsTableName, "fk_id")))
							.And(and => and.Reference(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "schema"))
								.In(@in => @in
									.Query(q => q
										.Column("name")
										.FromTable(InformationSchema.ThisUserSchemaInfoViewName))))));

				// CREATE VIEW INFORMATION_SCHEMA.Cross_Reference AS
				//   SELECT NULL AS PKTABLE_CATALOG,
				//          fkey_info.ref_schema AS PKTABLE_SCHEMA,
				//          fkey_info.ref_table AS PKTABLE_NAME,
				//          fkey_cols.pcolumn AS PKCOLUMN_NAME,
				//          NULL AS FKTABLE_CATALOG,
				//          fkey_info.schema AS FKTABLE_SCHEMA,
				//          fkey_info.table AS FKTABLE_NAME,
				//          fkey_cols.fcolumn AS FKCOLUMN_NAME,
				//          fkey_cols.seq_no AS KEY_SEQ,
				//          I_FRULE_CONVERT(fkey_info.update_rule) AS UPDATE_RULE,
				//          I_FRULE_CONVERT(fkey_info.delete_rule) AS DELETE_RULE,
				//          fkey_info.name AS FK_NAME,
				//          NULL AS PK_NAME,
				//          fkey_info.deferred AS DEFERRABILITY
				//   FROM SYSTEM.fkey_info, SYSTEM.fkey_cols
				//   WHERE fkey_info.id = fkey_cols.fk_id
				//     AND fkey_info.schema IN ( SELECT name FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )

				systemQuery.CreateView(InformationSchema.CrossReference, query => query
					.Constant(null, "PKTABLE_CATALOG")
					.Column(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "ref_schema"), "PKTABLE_SCHEMA")
					.Column(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "ref_table"), "PKTABLE_NAME")
					.Column(new ObjectName(SystemSchema.ForeignKeyColumnsTableName, "pcolumn"), "PKCOLUMN_NAME")
					.Constant(null, "FKTABLE_CATALOG")
					.Column(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "schema"), "FKTABLE_SCHEMA")
					.Column(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "table"), "FKTABLE_NAME")
					.Column(new ObjectName(SystemSchema.ForeignKeyColumnsTableName, "fcolumn"), "FKCOLUMN_NAME")
					.Column(new ObjectName(SystemSchema.ForeignKeyColumnsTableName, "seq_no"), "KEY_SEQ")
					.Function("I_FRULE_CONVERT",
						new SqlExpression[] { SqlExpression.Reference(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "update_rule")) },
						"UPDATE_RULE")
					.Function("I_FRULE_CONVERT",
						new SqlExpression[] { SqlExpression.Reference(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "delete_rule")) },
						"DELETE_RULE")
					.Column(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "name"), "FK_NAME")
					.Constant(null, "PK_NAME")
					.Column(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "deferred"), "DEFERRABILITY")
					.From(source => source.Table(SystemSchema.ForeignKeyInfoTableName),
						source => source.Table(SystemSchema.ForeignKeyColumnsTableName))
					.Where(where =>
						where.Reference(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "id"))
							.Equal(eq => eq.Reference(new ObjectName(SystemSchema.ForeignKeyColumnsTableName, "fk_id")))
							.And(and => and.Reference(new ObjectName(SystemSchema.ForeignKeyInfoTableName, "schema"))
								.In(@in => @in
									.Query(q => q
										.Column("name")
										.FromTable(InformationSchema.ThisUserSchemaInfoViewName))))));

				GrantToPublic(systemQuery);
			}

			// TODO: Create views that don't check for user's rights
		}

		private static void GrantToPublic(IQuery query) {
			query.Access()
				.GrantOn(DbObjectType.View, InformationSchema.ThisUserSimpleGrantViewName, User.PublicName, PrivilegeSets.TableRead);
			query.Access()
				.GrantOn(DbObjectType.View, InformationSchema.ThisUserGrantViewName, User.PublicName, PrivilegeSets.TableRead);
			query.Access()
				.GrantOn(DbObjectType.View, InformationSchema.ThisUserSchemaInfoViewName, User.PublicName, PrivilegeSets.TableRead);
			query.Access()
				.GrantOn(DbObjectType.View, InformationSchema.ThisUserTableInfoViewName, User.PublicName, PrivilegeSets.TableRead);
			query.Access()
				.GrantOn(DbObjectType.View, InformationSchema.ThisUserTableColumnsViewName, User.PublicName, PrivilegeSets.TableRead);

			query.Access().GrantOn(DbObjectType.View, InformationSchema.Catalogs, User.PublicName, PrivilegeSets.TableRead);
			query.Access().GrantOn(DbObjectType.View, InformationSchema.Schemata, User.PublicName, PrivilegeSets.TableRead);
			query.Access().GrantOn(DbObjectType.View, InformationSchema.Tables, User.PublicName, PrivilegeSets.TableRead);
			query.Access().GrantOn(DbObjectType.View, InformationSchema.TablePrivileges, User.PublicName, PrivilegeSets.TableRead);
			query.Access().GrantOn(DbObjectType.View, InformationSchema.Columns, User.PublicName, PrivilegeSets.TableRead);
			query.Access().GrantOn(DbObjectType.View, InformationSchema.ColumnPrivileges, User.PublicName, PrivilegeSets.TableRead);
			query.Access().GrantOn(DbObjectType.View, InformationSchema.PrimaryKeys, User.PublicName, PrivilegeSets.TableRead);
			query.Access().GrantOn(DbObjectType.View, InformationSchema.ImportedKeys, User.PublicName, PrivilegeSets.TableRead);
			query.Access().GrantOn(DbObjectType.View, InformationSchema.ExportedKeys, User.PublicName, PrivilegeSets.TableRead);
			query.Access().GrantOn(DbObjectType.View, InformationSchema.CrossReference, User.PublicName, PrivilegeSets.TableRead);
		}

		private static void OnCompositeCreate(IQuery systemQuery) {
			// SYSTEM.SCHEMA_INFO
			systemQuery.Access().CreateTable(table => table
				.Named(SystemSchema.SchemaInfoTableName)
				.WithColumn("id", PrimitiveTypes.Numeric())
				.WithColumn("name", PrimitiveTypes.String())
				.WithColumn("type", PrimitiveTypes.String())
				.WithColumn("culture", PrimitiveTypes.String())
				.WithColumn("other", PrimitiveTypes.String()));

			//var tableInfo = new TableInfo(SystemSchema.SchemaInfoTableName);
			//tableInfo.AddColumn("id", PrimitiveTypes.Numeric());
			//tableInfo.AddColumn("name", PrimitiveTypes.String());
			//tableInfo.AddColumn("type", PrimitiveTypes.String());
			//tableInfo.AddColumn("culture", PrimitiveTypes.String());
			//tableInfo.AddColumn("other", PrimitiveTypes.String());
			//tableInfo = tableInfo.AsReadOnly();
			//systemQuery.Access().CreateTable(tableInfo);

			// TODO: Move this to the setup phase?
			CreateSystemSchema(systemQuery);
		}

		private static void CreateSchema(IQuery systemQuery, string name, string type) {
			systemQuery.Access().CreateSchema(new SchemaInfo(name, type));
		}

		private static void CreateSystemSchema(IQuery systemQuery) {
			CreateSchema(systemQuery, SystemSchema.Name, SchemaTypes.System);
			CreateSchema(systemQuery, InformationSchema.SchemaName, SchemaTypes.System);
			CreateSchema(systemQuery, systemQuery.Session.Database().Context.DefaultSchema(), SchemaTypes.Default);
		}

		private static void OnBuild(ISystemBuilder builder) {
			builder.Use<IObjectManager>(options => options
				.With<SchemaManager>()
				.HavingKey(DbObjectType.Schema)
				.InTransactionScope());
		}
	}
}