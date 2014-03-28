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
using Deveel.Data.Types;

namespace Deveel.Data.Procedures {
	/// <summary>
	/// The parameter definition of a <see cref="StoredProcedure"/>.
	/// </summary>
	public sealed class ProcedureParameter : IStatementTreeObject {
		public ProcedureParameter(string name, TType type, ParameterDirection direction, bool nullable) {
			if (name == null)
				throw new ArgumentNullException("name");

			this.name = name;
			this.type = type;
			this.direction = direction;
			this.nullable = nullable;
		}

		private readonly string name;
		private readonly TType type;
		private ParameterDirection direction;
		private bool nullable;

		#region Implementation of ICloneable

		public bool IsNullable {
			get { return nullable; }
			set { nullable = value; }
		}

		public ParameterDirection Direction {
			get { return direction; }
			set { direction = value; }
		}

		public TType Type {
			get { return type; }
		}

		public string Name {
			get { return name; }
		}

		public object Clone() {
			throw new NotImplementedException();
		}

		public void PrepareExpressions(IExpressionPreparer preparer) {
			throw new NotImplementedException();
		}

		#endregion
	}
}