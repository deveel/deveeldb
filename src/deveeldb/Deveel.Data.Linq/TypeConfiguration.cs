using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Deveel.Data.Mapping;

namespace Deveel.Data.Linq {
	public class TypeConfiguration<T> : ITypeConfiguration where T : class {
		private KeyConfiguration keyConfiguration;
		private Dictionary<string, MemberConfiguration> members;
		private Dictionary<string, IAssociationConfiguration> associations;
		private string configSchema;
		private string configName;

		public TypeConfiguration() {
			members = new Dictionary<string, MemberConfiguration>();
			associations = new Dictionary<string, IAssociationConfiguration>();
		}

		public TypeConfiguration<T> HasName(string tableName) {
			return HasName(null, tableName);
		}

		public TypeConfiguration<T> HasName(string schema, string tableName) {
			configSchema = schema;
			configName = tableName;
			return this;
		}

		public KeyConfiguration HasKey<TMember>(Expression<Func<T, TMember>> selector) {
			var memberInfo = TypeUtil.SelectMember(selector);
			keyConfiguration = new KeyConfiguration(memberInfo);
			return keyConfiguration;
		}

		public MemberConfiguration Member<TMember>(Expression<Func<T, TMember>> selector) {
			var memberInfo = TypeUtil.SelectMember(selector);

			MemberConfiguration configuration;
			if (!members.TryGetValue(memberInfo.Name, out configuration)) {
				configuration = new MemberConfiguration(memberInfo);
				members[memberInfo.Name] = configuration;
			}

			return configuration;
		}

		DbTypeModel ITypeConfiguration.CreateModel() {
			var tableName = DiscoverTableName();
			var model = new DbTypeModel(typeof(T), tableName);
			foreach (var member in members.Values) {
				bool isKey = keyConfiguration != null && member.Member == keyConfiguration.Member;
				KeyType keyType = isKey ? keyConfiguration.KeyType : KeyType.None;
				model.AddMember((member as IMemberConfiguration).CreateModel(isKey, keyType));
			}

			foreach (var association in associations.Values) {
				
			}

			return model;
		}

		private string DiscoverTableName() {
			string tableName = null;
			if (!String.IsNullOrEmpty(configName)) {
				tableName = configName;
				if (!String.IsNullOrEmpty(configSchema))
					tableName = String.Format("{0}.{1}", configSchema, configName);
			} else if (Attribute.IsDefined(typeof(T), typeof(TableNameAttribute))) {
				var attr = (TableNameAttribute) Attribute.GetCustomAttribute(typeof(T), typeof(TableNameAttribute));
				tableName = attr.Name;
				if (!String.IsNullOrEmpty(tableName)) {
					var schema = attr.Schema;
					if (!String.IsNullOrEmpty(schema))
						tableName = String.Format("{0}.{1}", schema, tableName);
				}
			} else {
				// TODO: define a set of conventions for naming
				tableName = typeof(T).Name;
			}

			return tableName;
		}
	}
}
