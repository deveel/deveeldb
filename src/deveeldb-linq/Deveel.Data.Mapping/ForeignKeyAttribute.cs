using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Mapping {
	[AttributeUsage(AttributeTargets.Class | AttributeTargets.Field | AttributeTargets.Property)]
	public sealed class ForeignKeyAttribute : Attribute, INamedConstraint {
		// constructors for class attributes...
		public ForeignKeyAttribute(string name, Type referencedType, ForeignKeyAction onUpdate, ForeignKeyAction onDelete) {
			ConstraintName = name;
			ReferencedType = referencedType;
			OnUpdate = onUpdate;
			OnDelete = onDelete;
		}

		public ForeignKeyAttribute(Type referencedType, ForeignKeyAction onUpdate, ForeignKeyAction onDelete)
			: this(null, referencedType, onUpdate, onDelete) {
		}

		public ForeignKeyAttribute(string name, Type referencedType)
			: this(name, referencedType, ForeignKeyAction.Cascade, ForeignKeyAction.Cascade) {
		}

		public ForeignKeyAttribute(Type referencedType)
			: this(null, referencedType) {
		}

		// constructors for fields and properties...
		public ForeignKeyAttribute(Type referencedType, string referencedMember) {
			ReferencedType = referencedType;
			ReferencedMember = referencedMember;
		}

		public ForeignKeyAttribute(string name, string referencedMember) {
			ConstraintName = name;
			ReferencedMember = referencedMember;
		}

		public string ConstraintName { get; set; }

		public Type ReferencedType { get; private set; }

		public string ReferencedMember { get; private set; }

		public ForeignKeyAction OnUpdate { get; set; }

		public ForeignKeyAction OnDelete { get; set; }
	}
}