using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using DryIoc;

namespace Deveel.Data.Mapping {
	public sealed class Mapper {
		public static TypeMapInfo GetMapInfo(Type type) {
			if (type == null)
				throw new ArgumentNullException("type");

			var tableName = GetTableName(type);
			var typeMapInfo = new TypeMapInfo(type, tableName);

			var memberMapInfo = GetMembersMapInfo(type, typeMapInfo);
			foreach (var mapInfo in memberMapInfo) {
				typeMapInfo.AddMember(mapInfo);
			}

			return typeMapInfo;
		}

		public static TypeMapInfo GetMapInfo<T>() where T : class {
			return GetMapInfo(typeof (T));
		}

		public static object ToObject(Type type, Row row) {
			var mapInfo = GetMapInfo(type);
			return mapInfo.ToObject(row);
		}

		public static T ToObject<T>(Row row) where T : class {
			return (T) ToObject(typeof (T), row);
		}

		private static ObjectName GetTableName(Type type) {
#if PCL
			var attribute = type.GetTypeInfo().GetCustomAttribute<TableNameAttribute>();
			if (attribute != null) {
				ObjectName name;

				if (!String.IsNullOrEmpty(attribute.Schema)) {
					name = new ObjectName(new ObjectName(attribute.Schema), attribute.Name);
				} else {
					name = ObjectName.Parse(attribute.Name);
				}

				return name;
			}
#else
			if (Attribute.IsDefined(type, typeof (TableNameAttribute))) {
				var attribute = (TableNameAttribute) Attribute.GetCustomAttribute(type, typeof (TableNameAttribute));
				ObjectName name;

				if (!String.IsNullOrEmpty(attribute.Schema)) {
					name = new ObjectName(new ObjectName(attribute.Schema), attribute.Name);
				} else {
					name = ObjectName.Parse(attribute.Name);
				}

				return name;
			}
#endif

			return new ObjectName(type.Name);
		}

		private static IEnumerable<MemberMapInfo> GetMembersMapInfo(Type type, TypeMapInfo typeMapInfo) {
#if PCL
			var members = type.GetAllMembers().Where(x => !x.IsStatic());
#else
			var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);
			var fields = type.GetFields(BindingFlags.Instance | BindingFlags.Public);
			var members = properties.Cast<MemberInfo>().Union(fields);
#endif
			foreach (var memberInfo in members) {
#if PCL
				if (memberInfo.IsDefined(typeof(IgnoreAttribute)))
					continue;
#else
				if (Attribute.IsDefined(memberInfo, typeof(IgnoreAttribute)))
					continue;
#endif

				if (!(memberInfo is FieldInfo) &&
					!(memberInfo is PropertyInfo))
					continue;

				yield return GetMemberMapInfo(typeMapInfo, memberInfo);
			}
		}

		private static MemberMapInfo GetMemberMapInfo(TypeMapInfo typeMapInfo, MemberInfo memberInfo) {
			string columnName = memberInfo.Name;
			SqlType columnType = null;

			bool canBeNull = CanBeNull(memberInfo);
			bool nullable = canBeNull;

			object defaultValue = null;
			bool defaultIsExpression = false;

#if PCL
			if (memberInfo.IsDefined(typeof(ColumnAttribute))) {
				var attribute = memberInfo.GetCustomAttribute<ColumnAttribute>();
#else
			if (Attribute.IsDefined(memberInfo, typeof (ColumnAttribute))) {
				var attribute = (ColumnAttribute) Attribute.GetCustomAttribute(memberInfo, typeof (ColumnAttribute));
#endif
				if (!String.IsNullOrEmpty(attribute.Name))
					columnName = attribute.Name;

				if (!String.IsNullOrEmpty(attribute.TypeName)) {
					if (!PrimitiveTypes.IsPrimitive(attribute.TypeName))
						throw new NotSupportedException(String.Format("The type '{0}' is not supported.", attribute.TypeName));

					columnType = SqlType.Parse(attribute.TypeName);
				}

				defaultValue = attribute.Default;
				defaultIsExpression = attribute.DefaultIsExpression;

				nullable = attribute.Null;
			}

			if (nullable && !canBeNull)
				throw new NotSupportedException(String.Format("The member '{0}' is marked as NULL but is not nullable.", memberInfo.Name));

			if (columnType == null) {
				columnType = GetSqlType(memberInfo);
			}

			ConstraintMapInfo constraint = null;
#if PCL
			if (memberInfo.IsDefined(typeof (ConstraintAttribute))) {
				var attribute = memberInfo.GetCustomAttribute<ConstraintAttribute>();
#else
			if (Attribute.IsDefined(memberInfo, typeof (ConstraintAttribute))) {
				var attribute = (ConstraintAttribute) Attribute.GetCustomAttribute(memberInfo, typeof (ConstraintAttribute));
#endif
				constraint = new ConstraintMapInfo(memberInfo, columnName, attribute.Type, attribute.Expression);
			}

#if PCL
			if (memberInfo.IsDefined(typeof (IdentityAttribute))) {
#else
			if (Attribute.IsDefined(memberInfo, typeof (IdentityAttribute))) {
#endif
				if (constraint != null)
					throw new InvalidOperationException("Cannot specify an identity for a column that already defines a constraint.");

				if (defaultValue != null)
					throw new InvalidOperationException("Cannot specify an identity for a column that defines a default expression.");

				constraint = new ConstraintMapInfo(memberInfo, columnName, ConstraintType.PrimaryKey, null);
				defaultValue = String.Format("UNIQUEKEY('{0}')", typeMapInfo.TableName);
				defaultIsExpression = true;
			}

			var mapInfo = new MemberMapInfo(memberInfo, columnName, columnType, nullable, constraint);
			if (defaultValue != null) {
				mapInfo.SetDefault(defaultValue, defaultIsExpression);
			}
			return mapInfo;
		}

		private static bool CanBeNull(MemberInfo memberInfo) {
			Type memberType;
			if (memberInfo is PropertyInfo) {
				memberType = ((PropertyInfo)memberInfo).PropertyType;
			} else if (memberInfo is FieldInfo) {
				memberType = ((FieldInfo)memberInfo).FieldType;
			} else {
				return false;
			}

			if (memberType.IsValueType()) {
				var underlyingType = Nullable.GetUnderlyingType(memberType);
				if (underlyingType != null) {
					var nullableType = typeof (Nullable<>).MakeGenericType(underlyingType);
					return memberType == nullableType;
				}
				return false;
			}

			return !memberType.IsValueType();
		}

		private static SqlType GetSqlType(MemberInfo memberInfo) {
			Type memberType;
			if (memberInfo is PropertyInfo) {
				memberType = ((PropertyInfo) memberInfo).PropertyType;
			} else if (memberInfo is FieldInfo) {
				memberType = ((FieldInfo) memberInfo).FieldType;
			} else {
				throw new InvalidOperationException();
			}

			var underlyingType = Nullable.GetUnderlyingType(memberType);
			if (underlyingType != null)
				memberType = underlyingType;

			var typeCode = SqlType.GetTypeCode(memberType);
			if (!SqlType.IsPrimitiveType(typeCode))
				throw new NotSupportedException(String.Format("The field type '{0}' is not supported.", memberType));

			return PrimitiveTypes.FromType(memberType);
		}
	}
}
