using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Design.Configuration {
	public sealed class ConstraintModelConfiguration {
		private List<string> memberNames;

		internal ConstraintModelConfiguration(TypeModelConfiguration typeModel, string name, ConstraintType constraintType) {
			TypeModel = typeModel;
			ConstraintName = name;
			ConstraintType = constraintType;

			memberNames = new List<string>();
		}

		public ConstraintType ConstraintType { get; private set; }

		public string ConstraintName { get; private set; }

		public bool IsNamed {
			get { return !String.IsNullOrEmpty(ConstraintName); }
		}

		public TypeModelConfiguration TypeModel { get; private set; }

		public IEnumerable<string> MemberNames {
			get { return memberNames.ToArray(); }
		}

		public IEnumerable<MemberModelConfiguration> Members {
			get { return memberNames.Select(x => TypeModel.GetMember(x)); }
		}

		public SqlExpression CheckExpression { get; set; }

		public void AddMember(string memberName) {
			if (!TypeModel.HasMember(memberName))
				throw new InvalidOperationException();

			memberNames.Add(memberName);
		}

		public void RemoveMember(string memberName) {
			if (!TypeModel.HasMember(memberName))
				throw new InvalidOperationException();

			memberNames.Remove(memberName);
		}

		internal ConstraintModelConfiguration Clone(TypeModelConfiguration typeModel) {
			return new ConstraintModelConfiguration(typeModel, ConstraintName, ConstraintType) {
				memberNames = new List<string>(memberNames)
			};
		}
	}
}
