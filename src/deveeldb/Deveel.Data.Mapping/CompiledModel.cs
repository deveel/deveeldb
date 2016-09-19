using System;
using System.Collections.Generic;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Mapping {
	public sealed class CompiledModel {
		private readonly Dictionary<Type, TypeMapInfo> typeMap;

		internal CompiledModel() {
			typeMap = new Dictionary<Type, TypeMapInfo>();
		}

		internal void AddConfiguration(ITypeConfigurationProvider configuration) {
			var type = configuration.Type;
			var typeInfo = GenerateTypeInfo(configuration);

			typeMap[type] = typeInfo;
		}

		private TypeMapInfo GenerateTypeInfo(ITypeConfigurationProvider configuration) {
			throw new NotImplementedException();
		}

		internal string FindTableName(Type type) {
			TypeMapInfo mapInfo;
			if (!typeMap.TryGetValue(type, out mapInfo))
				throw new InvalidOperationException(String.Format("Type '{0}' is not mapped.", type));

			return mapInfo.TableName;
		}

		public TypeMapInfo GetTypeInfo<TType>() where TType : class {
			return GetTypeInfo(typeof(TType));
		}

		public TypeMapInfo GetTypeInfo(Type type) {
			if (type == null)
				throw new ArgumentNullException("type");
			if (!type.IsClass)
				throw new ArgumentException(String.Format("Type '{0}' is not a class", type));

			TypeMapInfo mapInfo;
			if (!typeMap.TryGetValue(type, out mapInfo))
				return null;

			return mapInfo;
		}

		internal object ToObject(Type destType, Row row) {
			var mapInfo = GetTypeInfo(destType);
			if (mapInfo == null)
				throw new ArgumentException(String.Format("The type '{0}' is not mapped in this context.", destType));

			return mapInfo.Construct(row);
		}
	}
}
