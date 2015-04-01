using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Types {
	[Serializable]
	public sealed class UserType : DataType, IDbObject {
		public UserType(UserTypeInfo typeInfo) 
			: base(typeInfo.TypeName.FullName, SqlTypeCode.UserType) {
			if (typeInfo == null)
				throw new ArgumentNullException("typeInfo");

			TypeInfo = typeInfo;
		}

		public UserTypeInfo TypeInfo { get; private set; }

		public ObjectName FullName {
			get { return TypeInfo.TypeName; }
		}

		DbObjectType IDbObject.ObjectType {
			get { return DbObjectType.Type; }
		}

		public override bool IsComparable(DataType type) {
			// For the moment not possible to compare
			return false;
		}

		public override bool CanCastTo(DataType type) {
			return false;
		}

		public override bool IsIndexable {
			get { return false; }
		}
	}
}
