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

using Deveel.Data.Types;

namespace Deveel.Data.Sql {
	/// <summary>
	/// An temporary implementation of <see cref="TType"/>
	/// used to handle parsing named types during a SQL
	/// statement (that defines an UDT by its name).
	/// </summary>
	internal class TNamedUserDefinedType : TType {
		public TNamedUserDefinedType(string typeName) 
			: base(SqlType.Object) {
			this.typeName = typeName;
		}

		private readonly string typeName;

		public string TypeName {
			get { return typeName; }
		}

		public override DbType DbType {
			get { return DbType.String; }
		}

		public override int Compare(object x, object y) {
			throw new InvalidOperationException();
		}

		public override bool IsComparableType(TType type) {
			throw new InvalidOperationException();
		}

		public override int CalculateApproximateMemoryUse(object ob) {
			throw new InvalidOperationException();
		}

		public override Type GetObjectType() {
			return typeof(string);
		}
	}
}