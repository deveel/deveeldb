// 
//  Copyright 2010-2016 Deveel
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

		public FetchContext(IRequest request, SqlExpression reference) 
			: this(request, FetchDirection.Next, reference) {
		}

		public FetchContext(IRequest request, FetchDirection direction, SqlExpression reference) {
			if (request == null)
				throw new ArgumentNullException("request");
			if (reference == null)
				throw new ArgumentNullException("reference");

			if (reference.ExpressionType != SqlExpressionType.VariableReference &&
				reference.ExpressionType != SqlExpressionType.Tuple &&
				reference.ExpressionType != SqlExpressionType.Reference)
				throw new ArgumentException("Invalid reference expression type.");

			Request = request;
			Direction = direction;
			Reference = reference;
		}

		public FetchDirection Direction { get; private set; }

		public SqlExpression Reference { get; private set; }

		public bool IsVariableReference {
			get {
				return Reference.ExpressionType == SqlExpressionType.VariableReference ||
				       Reference.ExpressionType == SqlExpressionType.Tuple;
			}
		}

		private string[] variables;

		public string[] VariableNames {
			get {
				if (variables == null) {
					if (!IsVariableReference)
						return new string[0];

					var varNames = new string[0];
					if (Reference is SqlVariableReferenceExpression) {
						varNames = new[] {((SqlVariableReferenceExpression) Reference).VariableName};
					} else if (Reference is SqlTupleExpression) {
						var varExpressions = ((SqlTupleExpression) Reference).Expressions;

						varNames = new string[varExpressions.Length];
						for (int i = 0; i < varNames.Length; i++) {
							var varRef = varExpressions[i] as SqlVariableReferenceExpression;
							if (varRef == null)
								throw new InvalidOperationException();

							varNames[i] = varRef.VariableName;
						}
					}

					variables = varNames;
				}

				return variables;
			}
		}

		public bool IsGlobalReference {
			get { return Reference.ExpressionType == SqlExpressionType.Reference; }
		}

		public IRequest Request { get; private set; }

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
