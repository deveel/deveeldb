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

using Deveel.Data.Sql;

namespace Deveel.Data.Routines {
	public abstract class Procedure : Routine, IProcedure {
		protected Procedure(ProcedureInfo procedureInfo)
			: base(procedureInfo) {
			if (procedureInfo == null)
				throw new ArgumentNullException("procedureInfo");

			ProcedureInfo = procedureInfo;
		}

		public ProcedureInfo ProcedureInfo { get; private set; }

		public ObjectName ProcedureName {
			get { return ProcedureInfo.RoutineName; }
		}

		IObjectInfo IDbObject.ObjectInfo {
			get { return ProcedureInfo; }
		}

		RoutineType IRoutine.Type {
			get { return RoutineType.Procedure; }
		}

		RoutineInfo IRoutine.RoutineInfo {
			get { return ProcedureInfo; }
		}
	}
}
