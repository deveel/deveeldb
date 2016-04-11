using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

using Deveel.Data.Sql;

namespace Deveel.Data.Mapping {
	public class TypeMap<TType> : ITypeMap where TType : class {
		private string TableName { get; set; }

		private readonly Dictionary<string, IMemberMap> members;

		public TypeMap() {
			members = new Dictionary<string, IMemberMap>();
		} 

		public TypeMap<TType> Table(string name) {
			TableName = name;
			return this;
		} 

		public MemberMap<TType> Column<TProperty>(Expression<Func<TType, TProperty>> member) {
			var memberInfo = GetMemberInfo(member);

			IMemberMap memberMap;
			if (!members.TryGetValue(memberInfo.Name, out memberMap)) {
				memberMap = new MemberMap<TType>(memberInfo);
				members[memberInfo.Name] = memberMap;
			}

			return (MemberMap<TType>) memberMap;
		}

		private MemberInfo GetMemberInfo<TProperty>(Expression<Func<TType, TProperty>> member) {
			throw new NotImplementedException();
		}

		TypeMapInfo ITypeMap.GetMapInfo() {
			var tableName = TableName;
			if (String.IsNullOrEmpty(tableName))
				tableName = typeof (TType).Name;

			var typeMapInfo = new TypeMapInfo(typeof(TType), ObjectName.Parse(tableName));

			foreach (var memberMap in members) {
				var mapInfo = memberMap.Value.GetMapInfo(typeMapInfo);
				typeMapInfo.AddMember(mapInfo);
			}

			return typeMapInfo;
		}
	}
}
