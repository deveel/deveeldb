using System;
using System.Collections.Generic;

using Deveel.Data.Types;

namespace Deveel.Data.Mapping {
	public sealed class TypeMapping {
		public TypeMapping(Type type, string tableName) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");

			Type = type;
			TableName = tableName;
			Members = new List<MemberMapping>();
		}

		public Type Type { get; private set; }

		public string TableName { get; private set; }

		public ICollection<MemberMapping> Members { get; private set; }

		public static TypeMapping CreateFor(TypeMappingRequest request) {
			if (request == null)
				throw new ArgumentNullException("request");

			var type = request.Type;

			var tableName = FindTableName(type, request.MappingContext);
			var members = FindMembers(type, request.MappingContext, request.TypeResolver);

			var mapping = new TypeMapping(type, tableName);
			foreach (var member in members) {
				mapping.Members.Add(member);
			}

			return mapping;
		}

		private static IEnumerable<MemberMapping> FindMembers(Type type, ITypeMappingContext mappingContext, ITypeResolver typeResolver) {
			throw new NotImplementedException();
		}

		private static string FindTableName(Type type, ITypeMappingContext mappingContext) {
			throw new NotImplementedException();
		}
	}
}
