// 
//  Copyright 2010-2011 Deveel
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
	/// An implementation of TType for a boolean value.
	/// </summary>
	[Serializable]
	public sealed class TBooleanType : TType {
		///<summary>
		///</summary>
		///<param name="sql_type"></param>
		public TBooleanType(SqlType sql_type)
			: base(sql_type) {
		}

		public override DbType DbType {
			get { return DbType.Boolean; }
		}

		/// <inheritdoc/>
		public override bool IsComparableType(TType type) {
			return (type is TBooleanType ||
					type is TNumericType);
		}

		/// <inheritdoc/>
		public override int Compare(object ob1, object ob2) {
			if (ob2 is BigNumber) {
				BigNumber n2 = (BigNumber)ob2;
				BigNumber n1 = !(bool)ob1 ?
								  BigNumber.Zero : BigNumber.One;
				return n1.CompareTo(n2);
			}

			if (ob1 == ob2 || ob1.Equals(ob2))
				return 0;
			if ((bool) ob1)
				return 1;
			return -1;
		}

		/// <inheritdoc/>
		public override int CalculateApproximateMemoryUse(Object ob) {
			return 5;
		}

		/// <inheritdoc/>
		public override Type GetObjectType() {
			return typeof(Boolean);
		}

	}
}