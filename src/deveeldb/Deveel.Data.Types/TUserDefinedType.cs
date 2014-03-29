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

using Deveel.Data.Sql;

namespace Deveel.Data.Types {
	/// <exclude/>
	public sealed class TUserDefinedType : TType {
		public TUserDefinedType(UserType userType) 
			: base(SqlType.Object) {
			this.userType = userType;
		}

		private readonly UserType userType;

		/// <summary>
		/// Gets the definition of the type.
		/// </summary>
		public UserType UserType {
			get { return userType; }
		}

		public override DbType DbType {
			get { return DbType.Object; }
		}

		public override int Compare(object x, object y) {
			throw new InvalidOperationException("Cannot compare two user-defined types.");
		}

		public override bool IsComparableType(TType type) {
			// it is not possible (yet) to compare UDTs
			return false;
		}

		public override int CalculateApproximateMemoryUse(object ob) {
			return 1000;
		}

		public override Type GetObjectType() {
			return typeof(UserObject);
		}
	}
}