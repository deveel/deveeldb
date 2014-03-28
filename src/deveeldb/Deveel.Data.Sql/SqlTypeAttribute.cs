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
	internal class SqlTypeAttribute : IStatementTreeObject {
		private readonly string name;
		private readonly TType type;
		private readonly bool not_null;

		public SqlTypeAttribute(string name, TType type, bool notNull) {
			this.name = name;
			not_null = notNull;
			this.type = type;
		}

		public string Name {
			get { return name; }
		}

		public TType Type {
			get { return type; }
		}

		public bool NotNull {
			get { return not_null; }
		}

		#region Implementation of ICloneable

		public object Clone() {
			return new SqlTypeAttribute(name, type, not_null);
		}

		public void PrepareExpressions(IExpressionPreparer preparer) {
		}

		#endregion
	}
}