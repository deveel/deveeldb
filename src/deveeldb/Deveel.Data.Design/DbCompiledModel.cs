using System;
using System.Collections.Generic;

using Deveel.Data.Linq;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Design {
	public sealed class DbCompiledModel {
		private readonly Dictionary<Type, DbTypeInfo> typeMap;
		private DbModelBuilder modelBuilder;

		internal DbCompiledModel(DbModelBuilder modelBuilder) {
			this.modelBuilder = modelBuilder;
			typeMap = new Dictionary<Type, DbTypeInfo>();
		}

		private DbTypeInfo FindType(Type type) {
			if (type == null)
				throw new ArgumentNullException("type");
			if (!type.IsClass)
				throw new ArgumentException(String.Format("Type '{0}' is not a class", type));

			DbTypeInfo mapInfo;
			if (!typeMap.TryGetValue(type, out mapInfo)) {
				var config = modelBuilder.ModelConfiguration.Type(type);
				if (config == null)
					throw new InvalidOperationException(String.Format("Type '{0}' is not mapped.", type));

				mapInfo = new DbTypeInfo(config);
				typeMap[type] = mapInfo;
			}

			return mapInfo;
		}

		internal string FindTableName(Type type) {
			var mapInfo = FindType(type);
			return mapInfo.TableName;
		}

		internal DbTypeInfo GetTypeInfo(Type type) {
			return FindType(type);
		}

		internal object ToObject(Type destType, Row row) {
			if (PrimitiveTypes.IsPrimitive(destType)) {
				if (row.ColumnCount > 1)
					throw new InvalidOperationException(String.Format("A result of type '{0}' cannot have more than one column.", destType));
				if (row.ColumnCount == 0)
					return null;

				return row[0].ConvertTo(destType);
			}

			var mapInfo = GetTypeInfo(destType);
			if (mapInfo == null)
				throw new ArgumentException(String.Format("The type '{0}' is not mapped in this context.", destType));

			return mapInfo.CreateObject(row);
		}

		public SessionQueryContext CreateContext(ISession session) {
			return new SessionQueryContext(session, this);
		}
	}
}
