using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Linq {
	public sealed class DbTypeModel : IDbModel {
		private Dictionary<string, DbColumnModel> members;
		private Dictionary<string, DbAssociationModel> associations;

		internal DbTypeModel(Type type, string tableName) {
			Type = type;
			TableName = tableName;
			members = new Dictionary<string, DbColumnModel>();
			associations = new Dictionary<string, DbAssociationModel>();
		}

		public Type Type { get; private set; }

		public string TableName { get; private set; }

		internal void AddMember(DbColumnModel memberModel) {
			if (associations.ContainsKey(memberModel.Member.Name))
				throw new ArgumentException(String.Format("The member '{0}' already declares an association.", memberModel.Member.Name));

			members[memberModel.Member.Name] = memberModel;
		}

		internal void AddAssociation(DbAssociationModel model) {
			if (members.ContainsKey(model.Member.Name))
				throw new ArgumentException(String.Format("The member '{0}' already declares a column", model.Member.Name));

			associations[model.Member.Name] = model;
		}

		public IEnumerable<string> MemberNames {
			get { return members.Keys.Union(associations.Keys); }
		}

		public bool IsMember(string name) {
			return members.ContainsKey(name);
		}

		public bool IsAssociation(string name) {
			return associations.ContainsKey(name);
		}

		public DbColumnModel GetColumn(string memberName) {
			DbColumnModel model;
			if (!members.TryGetValue(memberName, out model))
				return null;

			return model;
		}

		public DbAssociationModel GetAssociation(string memberName) {
			DbAssociationModel association;
			if (!associations.TryGetValue(memberName, out association))
				return null;

			return association;
		}
	}
}
