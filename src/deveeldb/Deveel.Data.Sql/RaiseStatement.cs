// 
//  Copyright 2011 Deveel
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
using System.Runtime.Serialization;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class RaiseStatement : Statement {
		private SqlState state;

		public RaiseStatement() {
		}

		private RaiseStatement(SerializationInfo info, StreamingContext context) {
			state = (SqlState) info.GetValue("State", typeof (SqlState));
		}

		public string StateName {
			get { return GetString("state_name"); }
			set {
				if (String.IsNullOrEmpty(value))
					throw new ArgumentNullException("value");
				SetValue("state_name", value);
			}
		}

		protected override void Prepare(IQueryContext context) {
			string stateName = GetString("state_name");
			state = SqlState.GetState(stateName);
		}

		protected override Table Evaluate(IQueryContext context) {
			throw state.AsException();
		}
	}
}