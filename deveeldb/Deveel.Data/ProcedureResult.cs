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

namespace Deveel.Data.Procedures {
	public sealed class ProcedureResult {
		internal ProcedureResult(StoredProcedure procedure) {
			this.procedure = procedure;
			outputParams = new Hashtable();
		}

		private readonly StoredProcedure procedure;

		private readonly Hashtable outputParams;
		private ProcedureException error;

		public StoredProcedure Procedure {
			get { return procedure; }
		}

		public bool HasOutputParameters {
			get { return outputParams.Count > 0; }
		}

		public bool IsErrorState {
			get { return error != null; }
		}

		public ProcedureException Error {
			get { return error; }
		}

		internal void SetError(ProcedureException e) {
			error = e;
		}

		internal void SetOutputParameter(string name, TObject value) {
			outputParams[name] = value;
		}

		public TObject GetOutputParameter(string name) {
			return outputParams[name] as TObject;
		}
	}
}