// 
//  Copyright 2010  Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;

using Deveel.Data.DbSystem;

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