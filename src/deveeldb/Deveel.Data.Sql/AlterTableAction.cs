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
using System.Collections;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Represents an action in an <c>ALTER TABLE</c> SQL statement.
	/// </summary>
	[Serializable]
	public sealed class AlterTableAction : IStatementTreeObject {
		/// <summary>
		/// Element parameters to do with the action.
		/// </summary>
		private ArrayList elements;

		/// <summary>
		/// The action to perform.
		/// </summary>
		private AlterTableActionType action;

		///<summary>
		///</summary>
		public AlterTableAction() {
			elements = new ArrayList();
		}

		/// <summary>
		/// Gets or sets the action to perform.
		/// </summary>
		public AlterTableActionType Action {
			set { action = value; }
			get { return action; }
		}

		/// <summary>
		/// Returns the ArrayList that represents the parameters of this action.
		/// </summary>
		public IList Elements {
			get { return elements; }
		}


		/// <inheritdoc/>
		void IStatementTreeObject.PrepareExpressions(IExpressionPreparer preparer) {
			// This must search throw 'elements' for objects that we can prepare
			for (int i = 0; i < elements.Count; ++i) {
				Object ob = elements[i];
				if (ob is String) {
					// Do not need to prepare this
				} else if (ob is Expression) {
					((Expression)ob).Prepare(preparer);
				} else if (ob is IStatementTreeObject) {
					((IStatementTreeObject)ob).PrepareExpressions(preparer);
				} else {
					throw new DatabaseException("Unrecognised expression: " + ob.GetType());
				}
			}
		}

		/// <inheritdoc/>
		public Object Clone() {
			// Shallow clone
			AlterTableAction v = (AlterTableAction)MemberwiseClone();
			ArrayList cloned_elements = new ArrayList();
			v.elements = cloned_elements;

			for (int i = 0; i < elements.Count; ++i) {
				Object ob = elements[i];
				if (ob is String) {
					// Do not need to clone this
				} else if (ob is Expression) {
					ob = ((Expression)ob).Clone();
				} else if (ob is IStatementTreeObject) {
					ob = ((IStatementTreeObject)ob).Clone();
				} else {
					throw new ApplicationException(ob.GetType().ToString());
				}
				cloned_elements.Add(ob);
			}

			return v;
		}

	}
}