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
