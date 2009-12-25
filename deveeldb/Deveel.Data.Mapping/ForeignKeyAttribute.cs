//  
//  ForeignKeyAttribute.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.


using System;

namespace Deveel.Data.Mapping {
	[AttributeUsage(AttributeTargets.Class | AttributeTargets.Field | AttributeTargets.Property)]
	public sealed class ForeignKeyAttribute : Attribute, INamedConstraint {
		// constructors for class attributes...
		public ForeignKeyAttribute(string name, Type referencedType, ConstraintAction onUpdate, ConstraintAction onDelete) {
			this.name = name;
			this.referencedType = referencedType;
			this.onUpdate = onUpdate;
			this.onDelete = onDelete;
		}

		public ForeignKeyAttribute(Type referencedType, ConstraintAction onUpdate, ConstraintAction onDelete)
			: this(null, referencedType, onUpdate, onDelete) {
		}

		public ForeignKeyAttribute(string name, Type referencedType)
			: this(name, referencedType, ConstraintAction.Cascade, ConstraintAction.Cascade) {
		}

		public ForeignKeyAttribute(Type referencedType)
			: this(null, referencedType) {
		}

		// constructors for fields and properties...
		public ForeignKeyAttribute(Type referencedType, string referencedMember) {
			this.referencedType = referencedType;
			this.referencedMember = referencedMember;
		}

		public ForeignKeyAttribute(string name, string referencedMember) {
			this.name = name;
			this.referencedMember = referencedMember;
		}

		private string name;
		private readonly Type referencedType;
		private readonly string referencedMember;
		private ConstraintAction onUpdate;
		private ConstraintAction onDelete;

		public string ConstraintName {
			get { return name; }
			set { name = value; }
		}

		public Type ReferencedType {
			get { return referencedType; }
		}

		public string ReferencedMember {
			get { return referencedMember; }
		}

		public ConstraintAction OnUpdate {
			get { return onUpdate; }
			set { onUpdate = value; }
		}

		public ConstraintAction OnDelete {
			get { return onDelete; }
			set { onDelete = value; }
		}
	}
}