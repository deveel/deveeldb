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
using System.Collections.Generic;

using Deveel.Data.Diagnostics;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Routines {
	public sealed class RoutineEvent : Event {
		public RoutineEvent(ObjectName routineName, InvokeArgument[] arguments) 
			: this(RoutineEventType.BeforeResolve, routineName, arguments, new RoutineType(), null) {
		}

		public RoutineEvent(RoutineEventType eventType, ObjectName routineName, InvokeArgument[] arguments, RoutineType routineType) 
			: this(eventType, routineName, arguments, routineType, null) {
		}

		public RoutineEvent(ObjectName routineName, InvokeArgument[] arguments, RoutineType routineType) 
			: this(RoutineEventType.BeforeExecute, routineName, arguments, routineType, null) {
		}

		public RoutineEvent(ObjectName routineName, InvokeArgument[] arguments, RoutineType routineType, InvokeResult result) 
			: this(RoutineEventType.AfterExecute, routineName, arguments, routineType, result) {
		}

		public RoutineEvent(RoutineEventType eventType, ObjectName routineName, InvokeArgument[] arguments, RoutineType routineType, InvokeResult result) {
			if (routineName == null)
				throw new ArgumentNullException("routineName");

			EventType = eventType;
			RoutineName = routineName;
			Arguments = arguments;
			RoutineType = routineType;
			Result = result;
		}

		public RoutineEventType EventType { get; private set; }

		public ObjectName RoutineName { get; private set; }

		public InvokeArgument[] Arguments { get; private set; }

		public RoutineType RoutineType { get; private set; }

		public InvokeResult Result { get; private set; }

		protected override void GetEventData(Dictionary<string, object> data) {
			data["routine.eventType"] = EventType.ToString();
			data["routine.name"] = RoutineName.FullName;

			if (Arguments != null) {
				data["routine.argc"] = Arguments.Length;
				for (int i = 0; i < Arguments.Length; i++) {
					data[String.Format("routine.arg[{0}].name", i)] = Arguments[i].Name;
					data[String.Format("routine.arg[{0}].value", i)] = Arguments[i].Value.ToString();
				}
			}

			if (EventType != RoutineEventType.BeforeResolve)
				data["routine.type"] = RoutineType.ToString();

			base.GetEventData(data);
		}
	}
}
