using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Design {
	public sealed class DbCompiledModel {
		private readonly Dictionary<Type, DbTypeInfo> typeMap;
		private DbModelBuilder modelBuilder;

		internal DbCompiledModel(DbModelBuilder modelBuilder) {
			this.modelBuilder = modelBuilder;
			typeMap = new Dictionary<Type, DbTypeInfo>();
		}

		//private TypeMapInfo GenerateTypeInfo(ITypeConfigurationProvider configuration) {
		//	var constraintMembers = new List<ConstraintMemberInfo>();

		//	var members = configuration.MemberConfigurations
		//		.Select(x => GenerateMemberInfo(x, constraintMembers))
		//		.Where(x => x != null);

		//	var primary = constraintMembers
		//		.Where(x => x.ConstraintType == ConstraintType.PrimaryKey)
		//		.Select(x => x.Member)
		//		.ToArray();

		//	var unique = constraintMembers
		//		.Where(x => x.ConstraintType == ConstraintType.Unique)
		//		.Select(x => x.Member)
		//		.ToArray();

		//	var constraints = new List<TypeConstraintMapInfo>();
		//	if (configuration.Check != null)
		//		constraints.Add(new TypeConstraintMapInfo(ConstraintType.Check, null, configuration.Check));

		//	if (primary.Any())
		//		constraints.Add(new TypeConstraintMapInfo(ConstraintType.PrimaryKey, primary.Select(x => members.First(y => y.Member.Name == x)), null));
		//	if (unique.Any())
		//		constraints.Add(new TypeConstraintMapInfo(ConstraintType.Unique, unique.Select(x => members.First(y => y.Member.Name == x)), null));

		//	return new TypeMapInfo(configuration.Type, configuration.TableName, members, constraints);
		//}

		//private class ConstraintMemberInfo {
		//	public string Member { get; set; }

		//	public ConstraintType ConstraintType { get; set; }
		//}

		//private TypeMemberMapInfo GenerateMemberInfo(IMemberConfigurationProvider configuration, IList<ConstraintMemberInfo> constraints) {
		//	if (configuration.IsIgnored)
		//		return null;

		//	if (configuration.IsPrimaryKey) {
		//		constraints.Add(new ConstraintMemberInfo {
		//			ConstraintType = ConstraintType.PrimaryKey,
		//			Member = configuration.Member.Name
		//		});
		//	} else if (configuration.IsUnique) {
		//		constraints.Add(new ConstraintMemberInfo {
		//			ConstraintType = ConstraintType.Unique,
		//			Member = configuration.Member.Name
		//		});
		//	}

		//	return new TypeMemberMapInfo(configuration.Member, configuration.ColumnName, configuration.ColumnType, configuration.IsNotNull, configuration.DefaultExpression);
		//}

		internal string FindTableName(Type type) {
			DbTypeInfo mapInfo;
			if (!typeMap.TryGetValue(type, out mapInfo))
				throw new InvalidOperationException(String.Format("Type '{0}' is not mapped.", type));

			return mapInfo.TableName;
		}

		internal DbTypeInfo GetTypeInfo(Type type) {
			if (type == null)
				throw new ArgumentNullException("type");
			if (!type.IsClass)
				throw new ArgumentException(String.Format("Type '{0}' is not a class", type));

			DbTypeInfo mapInfo;
			if (!typeMap.TryGetValue(type, out mapInfo))
				return null;

			return mapInfo;
		}

		internal object ToObject(Type destType, Row row) {
			var mapInfo = GetTypeInfo(destType);
			if (mapInfo == null)
				throw new ArgumentException(String.Format("The type '{0}' is not mapped in this context.", destType));

			return mapInfo.CreateObject(row);
		}
	}
}
