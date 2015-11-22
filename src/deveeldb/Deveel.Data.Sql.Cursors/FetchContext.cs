// 
//  Copyright 2010-2015 Deveel
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
//

using System;

using Deveel.Data;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Cursors {
	public sealed class FetchContext {
		private int offset;

		public FetchContext(IQuery query, SqlExpression reference) 
			: this(query, FetchDirection.Next, reference) {
		}

		public FetchContext(IQuery query, FetchDirection direction, SqlExpression reference) {
			if (query == null)
				throw new ArgumentNullException("query");
			if (reference == null)
				throw new ArgumentNullException("reference");

			if (reference.ExpressionType != SqlExpressionType.VariableReference &&
				reference.ExpressionType != SqlExpressionType.Reference)
				throw new ArgumentException("Invalid reference expression type.");

			Query = query;
			Direction = direction;
			Reference = reference;
		}

		public FetchDirection Direction { get; private set; }

		public SqlExpression Reference { get; set; }

		public bool IsVariableReference {
			get { return Reference.ExpressionType == SqlExpressionType.VariableReference; }
		}

		public bool IsGlobalReference {
			get { return Reference.ExpressionType == SqlExpressionType.Reference; }
		}

		public IQuery Query { get; private set; }

		public int Offset {
			get { return offset; }
			set {
				if (Direction != FetchDirection.Absolute &&
					Direction != FetchDirection.Relative)
					throw new ArgumentException("Cannot set offset for this direction.");

				offset = value;
			}
		}
	}
}
