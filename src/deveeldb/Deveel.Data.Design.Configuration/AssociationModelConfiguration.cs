using System;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Design.Configuration {
	public sealed class AssociationModelConfiguration {
		internal AssociationModelConfiguration(TypeModelConfiguration typeModel, string memberName, AssociationType associationType) {
			TypeModel = typeModel;
			MemberName = memberName;
			AssociationType = associationType;
		}

		public TypeModelConfiguration TypeModel { get; private set; }

		public string MemberName { get; private set; }

		public MemberModelConfiguration SourceMember {
			get { return TypeModel.GetMember(MemberName); }
		}

		public AssociationType AssociationType { get; private set; }

		public AssociationCardinality Cardinality { get; set; }

		public MemberModelConfiguration TargetMember { get; set; }

		public TypeModelConfiguration TargetType {
			get { return TargetMember == null ? null : TargetMember.TypeModel; }
		}

		public MemberModelConfiguration KeyMember { get; set; }

		public ForeignKeyAction DeleteAction { get; set; }

		public AssociationFunction Function { get; set; }

		internal AssociationModelConfiguration Clone(TypeModelConfiguration typeModel) {
			var model = typeModel.Model;

			var targetMember = TargetMember;
			if (targetMember != null) {
				var targetModel = model.Type(targetMember.TypeModel.Type);
				targetMember = targetModel.GetMember(targetMember.Member.Name);
			}

			var keyMember = KeyMember;
			if (keyMember != null) {
				keyMember = typeModel.GetMember(keyMember.Member.Name);
			}

			return new AssociationModelConfiguration(typeModel, MemberName, AssociationType) {
				TargetMember = targetMember,
				Cardinality = Cardinality,
				DeleteAction = DeleteAction,
				Function = Function,
				KeyMember = keyMember
			};
		}
	}
}
