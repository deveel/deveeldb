using System;
using System.Collections.Generic;
using System.Reflection;

using Deveel.Data.Sql.Types;

using IQToolkit;

namespace Deveel.Data.Mapping {
	public sealed class MappingContext {
		private Dictionary<Type, ITypeMappingConfiguration> configurations;

		internal MappingContext() {
			TableNameConvention = RuledNamingConvention.SqlNaming;
			ColumnNameConvention = RuledNamingConvention.SqlNaming;
		}

		public INamingConvention TableNameConvention { get; set; }

		public INamingConvention ColumnNameConvention { get; set; }

		public TypeMappingConfiguration<T> Map<T>() where T : class {
			return Map<T>(null);
		}

		public TypeMappingConfiguration<T> Map<T>(TypeMappingConfiguration<T> configuration) where T : class {
			if (configurations == null)
				configurations = new Dictionary<Type, ITypeMappingConfiguration>();

			var type = typeof (T);

			if (configuration == null) {
				ITypeMappingConfiguration config;
				if (!configurations.TryGetValue(type, out config)) {
					configuration = new TypeMappingConfiguration<T>();
					configurations[type] = configuration;
				} else {
					configuration = (TypeMappingConfiguration<T>) config;
				}
			} else {
				configurations[type] = configuration;
			}

			return configuration;
		}

		internal MappingModel CreateModel() {
			var model = new MappingModel();
			if (configurations != null) {
				foreach (var configuration in configurations.Values) {
					var mapping = CreateTypeMapping(model, configuration);

					// TODO: make relationships...

					model.Map(mapping);
				}
			}

			return model;
		}

		private string GetTableName(ITypeMappingConfiguration configuration) {
			var tableName = configuration.TableName;
			if (String.IsNullOrEmpty(tableName))
				tableName = configuration.ElementType.Name;

			if (TableNameConvention != null)
				tableName = TableNameConvention.FormatName(tableName);

			return tableName;
		}

		private TypeMapping CreateTypeMapping(MappingModel model, ITypeMappingConfiguration configuration) {
			var tableName = GetTableName(configuration);
			var mapping = new TypeMapping(model, configuration.ElementType, tableName);

			foreach (var pair in configuration.Members) {
				var member = pair.Value;
				var memberMapping = MapMember(mapping, member);
				mapping.AddMember(memberMapping);
			}

			return mapping;
		}

		private MemberMapping MapMember(TypeMapping mapping, IMemberMappingConfiguration configuration) {
			// TODO: unique key, that is not UNIQUE
			var sqlType = FormSqlType(configuration);
			var notNull = (configuration.ColumnConstraints & ColumnConstraints.NotNull) != 0;
			var primary = (configuration.ColumnConstraints & ColumnConstraints.PrimaryKey) != 0;
			var unique = (configuration.ColumnConstraints & ColumnConstraints.Unique) != 0;
			bool uniqueKey = mapping.IsUniqueKey(configuration.Member.Name);
			var columnName = GetColumnName(configuration);
			return new MemberMapping(mapping, configuration.Member, columnName, sqlType, notNull, primary, unique, uniqueKey);
		}

		private string GetColumnName(IMemberMappingConfiguration configuration) {
			var columnName = configuration.ColumnName;
			if (String.IsNullOrEmpty(columnName))
				columnName = configuration.Member.Name;

			if (ColumnNameConvention != null)
				columnName = ColumnNameConvention.FormatName(columnName);

			return columnName;
		}

		private SqlType FormSqlType(IMemberMappingConfiguration configuration) {
			var sqlType = configuration.ColumnType;
			if (sqlType == null)
				sqlType = DiscoverSqlType(configuration.Member);

			var meta = new List<DataTypeMeta>();
			if (configuration.Size != null)
				meta.Add(new DataTypeMeta("Size", configuration.Size.Value.ToString()));
			if (configuration.Precision != null)
				meta.Add(new DataTypeMeta("Precision", configuration.Precision.Value.ToString()));

			return SqlType.Resolve(sqlType.Value, meta.ToArray());
		}

		private SqlTypeCode DiscoverSqlType(MemberInfo member) {
			var memberType = TypeHelper.GetMemberType(member);
			if (memberType == typeof(bool))
				return SqlTypeCode.Boolean;
			if (memberType == typeof(byte))
				return SqlTypeCode.TinyInt;
			if (memberType == typeof(short))
				return SqlTypeCode.SmallInt;
			if (memberType == typeof(int))
				return SqlTypeCode.Integer;
			if (memberType == typeof (long))
				return SqlTypeCode.BigInt;

			if (memberType == typeof(string))
				return SqlTypeCode.VarChar;

			throw new NotSupportedException();
		}
	}
}
