using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Design {
	public sealed class TypeMapInfo {
		internal TypeMapInfo(Type type, string tableName, IEnumerable<TypeMemberMapInfo> members, IEnumerable<TypeConstraintMapInfo> constraints) {
			Type = type;
			TableName = tableName;
			Members = members;
			Constraints = constraints;
		}

		public Type Type { get; private set; }

		public string TableName { get; private set; }

		public IEnumerable<TypeMemberMapInfo> Members { get; private set; }

		public IEnumerable<TypeConstraintMapInfo> Constraints { get; private set; }

		internal TypeMemberMapInfo FindMemberMap(string memberName) {
			if (Members == null)
				return null;

			var members = Members.ToDictionary(x => x.Member.Name, y => y);
			TypeMemberMapInfo memberInfo;

			if (!members.TryGetValue(memberName, out memberInfo))
				return null;

			return memberInfo;
		}

		internal object Construct(Row row) {
			var obj = Activator.CreateInstance(Type);

			foreach (var mapInfo in Members) {
				var sourceValue = row[mapInfo.ColumnName];

				if (mapInfo.Member is PropertyInfo) {
					var propInfo = (PropertyInfo) mapInfo.Member;
					propInfo.SetValue(obj, sourceValue.ConvertTo(propInfo.PropertyType), null);
				} else if (mapInfo.Member is FieldInfo) {
					var fieldInfo = (FieldInfo) mapInfo.Member;
					fieldInfo.SetValue(obj, sourceValue.ConvertTo(fieldInfo.FieldType));
				}
			}

			return obj;
		}
	}
}
