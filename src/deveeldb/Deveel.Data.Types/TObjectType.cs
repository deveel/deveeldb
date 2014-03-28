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

namespace Deveel.Data.Types {
	/// <summary>
	/// An implementation of TType for a object of possibly defined type.
	/// </summary>
	[Serializable]
	public class TObjectType : TType {
		/// <summary>
		/// The type of class this is contrained to or null if it is not constrained to a <see cref="Type"/>.
		/// </summary>
		private readonly String type_name;

		public TObjectType(String type_name)
			: base(SqlType.Object) {
			this.type_name = type_name;
		}

		public TObjectType(Type type)
			: this(type.FullName) {
		}

		/// <summary>
		/// Gets the string describing the <see cref="Type"/>.
		/// </summary>
		public string TypeString {
			get { return type_name; }
		}

		/// <inheritdoc/>
		public override bool IsComparableType(TType type) {
			return (type is TObjectType);
		}

		public override DbType DbType {
			get { return DbType.Object; }
		}

		/// <inheritdoc/>
		public override int Compare(Object ob1, Object ob2) {
			throw new ApplicationException("Object types can not be compared.");
		}

		/// <inheritdoc/>
		public override int CalculateApproximateMemoryUse(Object ob) {
			if (ob != null)
				return ((ByteLongObject) ob).Length + 4;
			return 4 + 8;
		}

		/// <inheritdoc/>
		public override Type GetObjectType() {
			return typeof(ByteLongObject);
		}
	}
}