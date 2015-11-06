using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Mapping {
	public class TypeMapping {
		private readonly Dictionary<string, MemberMapping> memberMappings;

		internal TypeMapping(MappingModel model, Type type, string tableName) {
			Model = model;
			Type = type;
			TableName = tableName;
			memberMappings = new Dictionary<string, MemberMapping>();
		}

		public MappingModel Model { get; private set; }

		public Type Type { get; private set; }

		public string TableName { get; private set; }

		public IEnumerable<MemberMapping> Members {
			get { return memberMappings.Values.AsEnumerable(); }
		}

		public IEnumerable<string> MemberNames {
			get { return memberMappings.Keys.AsEnumerable(); }
		} 

		internal void AddMember(MemberMapping mapping) {
			memberMappings[mapping.MemberName] = mapping;
		}

		public MemberMapping GetMember(string memberName) {
			if (String.IsNullOrEmpty(memberName))
				throw new ArgumentNullException("memberName");

			MemberMapping mapping;
			if (!memberMappings.TryGetValue(memberName, out mapping))
				return null;

			return mapping;
		}

		public bool IsMemberMapped(string memberName) {
			if (String.IsNullOrEmpty(memberName))
				throw new ArgumentNullException("memberName");

			return memberMappings.ContainsKey(memberName);
		}

		public bool IsColumn(string memberName) {
			// TODO: handle also relationships in the TypeMapping
			return IsMemberMapped(memberName);
		}

		public bool IsPrimaryKey(string memberName) {
			var member = GetMember(memberName);
			if (member == null)
				return false;

			return member.PrimaryKey;
		}

		public bool IsUniqueKey(string memberName) {
			var member = GetMember(memberName);
			if (member == null)
				return false;

			return member.UniqueKey;
		}
	}
}
